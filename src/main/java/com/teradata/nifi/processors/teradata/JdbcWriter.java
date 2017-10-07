/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Wrap JDBC commands, used by the processor.
 * Note that this function still is not thread save a JDBC itself is not thread save.
 * <H2>What is wrapped?</H2>
 * The wrapped methods are:<br>
 * <ul>
 * <li>prepare Statement. Prepares Statement against database and returns prepared statement and eventual error codes.</li>
 * <li>bind value to column of row. Executes asynchronous.</li>
 * <li>put row into batch. Executes asynchronous. Relies on all binds to be finished first.</li>
 * <li>execute Batch of prepared statement. Waits to finish on database and returns results and error codes.</li>
 * <li>execute SQL. Waits to finish on database and returns results and error codes.</li>
 * </ul>
 * <br>
 * @author juergenb
 */
class JdbcWriter implements AutoCloseable {
	public interface JdbcBinder {
		void bind(PreparedStatement preparedStatement, int parameterIndex, Record record, String fieldName) throws SQLException, NumberFormatException;
		int getSqlType();
	}

	private String beforeScript, afterScript, eltScript, insertStatement;
	private Connection scriptConnection, loadConnection;
	private PreparedStatement preparedStatement = null;
	private Column[] preparedColumns;
	private ComponentLog logger;
	
	/**
	 * @param scriptConnection: Must be an open JDBC connection to run SQL Scripts over it.
	 * 	This connection is used to execute the before- and after scripts.
	 * @param loadConnection: Must be an open JDBC connection configured for fast bulk load of data via prepared statements.
	 * 	You probably want to use TYPE=FASTLOAD in the URL and some other performance parameter.
	 * 	See http://developer.teradata.com/doc/connectivity/jdbc/reference/current/frameset.html to get some good ideas.
	 * @param insertStatement: Insert statement to be prepared for fast bulk inserts via loadConnection.
	 * @param beforeScript: To run before load into table.
	 * 	Most likely a delete all or create table statement to ensure an empty table for fastload.
	 * @param afterScript: To run after given number of rows loaded into table or a specified time elapsed.
	 * 	Most likely some SQL statement to run ELT on the loaded data.
	 * @throws SQLException 
	 */
	public JdbcWriter(
			Connection scriptConnection, Connection loadConnection,
			String beforeScript, String insertStatement, String eltScript, String afterScript,
			ComponentLog logger) throws SQLException {
		
		this.logger = logger;
		
		// Is connection ok?
		logWarnings(scriptConnection);
		logWarnings(loadConnection);
		
		this.scriptConnection = scriptConnection;
		this.loadConnection = loadConnection;
		this.beforeScript = beforeScript;
		this.eltScript = eltScript;
		this.afterScript = afterScript;
		this.insertStatement = insertStatement;
		
		// We do commit only after each load has finished
		loadConnection.setAutoCommit(false);
		// scriptConnection.setAutoCommit(false);
		
		// Is connection and statement OK?
		logWarnings(scriptConnection);
		logWarnings(loadConnection);
	}
	
	public String runBeforeScript() throws SQLException {return runScript(beforeScript);}
	public String runEltScript() throws SQLException {return runScript(eltScript);}
	public String runAfterScript() throws SQLException {return runScript(afterScript);}

	public void addRow(Object[] values) throws SQLException {
		if(values.length != preparedColumns.length) {
			throw new IllegalArgumentException("values.length: " + values.length + ", expected: " + preparedColumns.length + " per number of parameters in prepared statement");
		}
		
		// Prepare statement as late as possible to avoid locks on table when before script is running.
		prepare(loadConnection, insertStatement);
		
		rowStart(preparedStatement);
		for(int i=0; i<preparedColumns.length; i++) preparedColumns[i].bind(preparedStatement, i+1, values[i]);
		rowEnd(preparedStatement);
	}
	
	public void transferBatch() throws SQLException {transferBatch(preparedStatement);}
	public void commit() throws SQLException {commit(preparedStatement, loadConnection);}

	@Override
	public void close() throws SQLException {
		SQLException e = null;
		e = close(scriptConnection, e);
		e = close(loadConnection, e);
		
		if(e!= null) throw e;
	}
	
	public String getURL() throws SQLException {return loadConnection.getMetaData().getURL();}
	
	private void rowStart(PreparedStatement preparedStatement) throws SQLException {
		preparedStatement.clearParameters();
		logWarnings(preparedStatement);
	}
	
	private void rowEnd(PreparedStatement preparedStatement) throws SQLException {
		preparedStatement.addBatch();
		logWarnings(preparedStatement);
	}
	
	/**
	 * @param preparedStatement with existing batch to be transfered.
	 * @return Number of rows inserted over all batches or a negative number indicating error (ERROR_FAILURE) or unknown result (SUCCESS_UNKNOWN).
	 * @throws SQLException 
	 */
	private void transferBatch(PreparedStatement preparedStatement) throws SQLException {
		preparedStatement.executeBatch();
		logWarnings(preparedStatement);
		
		preparedStatement.clearBatch();
		logWarnings(preparedStatement);
	}
	
	/**
	 * @param connection to be committed.
	 * @throws SQLException
	 */
	private void commit(PreparedStatement preparedStatement, Connection connection) throws SQLException {
		connection.commit();
		preparedStatement.close();
	}
	
	/**
	 * @param connection to be closed
	 * @param root a chain of SQLExceptions happened so far. Null if all operations were successful so far.
	 * @return a chained SQLException if anything went wrong. On success root is returned unchanged.
	 */
	private SQLException close(Connection connection, SQLException root) {
		try {
			connection.close();
		} catch(SQLException e) {
			if(root != null) {
				root.setNextException(e);
				return root;
			} else {
				return e;
			}
		}
		return root;
	}
	
	/**
	 * @param script with SQL statements to run against open scriptConnection.
	 * @return ResulSets, update counts and ... as JSON document.
	 * @throws SQLException 
	 */
	private String runScript(String script) throws SQLException {
		JSONArray json = new JSONArray();
		if(script != null) try (Statement statement = scriptConnection.createStatement()) {
			logWarnings(statement);
			executeOneScript(script, statement, json);
		}
		if(!scriptConnection.getAutoCommit()) scriptConnection.commit();
		return json.toString();
	}
	
	private void executeOneScript(String script, Statement statement, JSONArray json) throws SQLException {
		for(boolean isRows = statement.execute(script); true; isRows = statement.getMoreResults()) {
			JSONObject jsonOneResult = new JSONObject();
			
			if(isRows) {
				JSONArray jsonRows = new JSONArray();
				try (ResultSet resultSet = statement.getResultSet()) {
					logWarnings(resultSet);
					
					ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
					
					while(resultSet.next()) {
						JSONObject jsonRow = new JSONObject();
						for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
							jsonRow.put(resultSetMetaData.getColumnName(i), resultSet.getObject(i));
						}
						jsonRows.put(jsonRow);
					}
					
					jsonOneResult.put("Rows", jsonRows);
				}
			} else {
				int updateCount = statement.getUpdateCount();
				if(updateCount == -1) {break;}
				
				jsonOneResult.put("UpdateCount", updateCount);
			}
			json.put(jsonOneResult);
		}
	}
	
	/**
	 * @param connection to print warnings for.
	 * @throws SQLException
	 */
	private void logWarnings(Connection connection) throws SQLException {
		logWarnings(connection.getWarnings());
		connection.clearWarnings();
	}
	
	/**
	 * @param statement to print warnings for.
	 * @throws SQLException
	 */
	private void logWarnings(Statement statement) throws SQLException {
		logWarnings(statement.getWarnings());
		statement.clearWarnings();
	}
	
	/**
	 * @param resultSet to print warnings for.
	 * @throws SQLException
	 */
	private void logWarnings(ResultSet resultSet) throws SQLException {
		logWarnings(resultSet.getWarnings());
		resultSet.clearWarnings();
	}
	
	/**
	 * @param warning as starting point to print chain of warnings.
	 * @throws SQLException
	 */
	private void logWarnings(SQLWarning warning) throws SQLException {
		for(; warning != null; warning = warning.getNextWarning()) {
			logger.warn(
					warning.getMessage() + "(SQL State = " + warning.getSQLState() + "), (error code = " + warning.getErrorCode() + ")",
					warning);
		}
	}
	
	// Set preparedStatement and preparedColumns
	private void prepare(Connection connection, String insertStatement) throws SQLException {
		if(preparedStatement != null) return; // Already done
		preparedStatement = connection.prepareStatement(insertStatement);
		
		ParameterMetaData parameterMetaData = preparedStatement.getParameterMetaData();
		preparedColumns = new Column[parameterMetaData.getParameterCount()];
		for(int i=0; i<preparedColumns.length; i++) {
			preparedColumns[i] = new Column(parameterMetaData, i+1);
		}
	}
	
	private class Column {
		private int targetSqlType;
		private int scaleOrLength;
		
		public Column(ParameterMetaData parameterMetaData, int parameterIndex) throws SQLException  {
			this.targetSqlType = parameterMetaData.getParameterType(parameterIndex);
			if((this.scaleOrLength = parameterMetaData.getScale(parameterIndex)) == 0)
				this.scaleOrLength = parameterMetaData.getPrecision(parameterIndex);
		}
		
		public void bind(PreparedStatement preparedStatement, int parameterIndex, Object value) throws SQLException {
			preparedStatement.setObject(parameterIndex, value, targetSqlType, scaleOrLength);
		}
	}
}
