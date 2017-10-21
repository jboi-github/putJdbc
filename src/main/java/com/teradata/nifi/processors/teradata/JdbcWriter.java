package com.teradata.nifi.processors.teradata;

import org.apache.nifi.logging.ComponentLog;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.sql.*;

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
	 */
	JdbcWriter(
			Connection scriptConnection, Connection loadConnection,
			String beforeScript, String insertStatement, String eltScript, String afterScript,
			ComponentLog logger) throws SQLException {
		
		this.logger = logger;

		// Is connection OK?
		logWarnings(scriptConnection);
		logWarnings(loadConnection);
		
		this.scriptConnection = scriptConnection;
		this.loadConnection = loadConnection;
		
		resetSQL(beforeScript, insertStatement, eltScript, afterScript);
		
		// We do commit only after each load has finished
		loadConnection.setAutoCommit(false);
		// scriptConnection.setAutoCommit(false);
		
		// Is connection and statement OK?
		logWarnings(scriptConnection);
		logWarnings(loadConnection);
	}
	
	JSONArray runBeforeScript() throws SQLException {return runScript(beforeScript);}
	JSONArray runEltScript() throws SQLException {return runScript(eltScript);}
	JSONArray runAfterScript() throws SQLException {return runScript(afterScript);}

	void addRow(Object[] values) throws SQLException {
		// Prepare statement as late as possible to avoid locks on table when before script is running.
		prepare(loadConnection, insertStatement);
		
		if(values.length != preparedColumns.length) {
			throw new IllegalArgumentException("values.length: " + values.length + ", expected: " + preparedColumns.length + " per number of parameters in prepared statement");
		}
		
		rowStart(preparedStatement);
		for(int i=0; i<preparedColumns.length; i++) preparedColumns[i].bind(preparedStatement, i+1, values[i]);
		rowEnd(preparedStatement);
	}

	int transferCsv(InputStream in) throws SQLException {
		try (PreparedStatement preparedStatement = loadConnection.prepareStatement(insertStatement)) {
			logWarnings(preparedStatement);
			
			preparedStatement.setAsciiStream(1, in, -1);
			logWarnings(preparedStatement);
			
			int updateCount = preparedStatement.executeUpdate();
			logWarnings(preparedStatement);
			
			return updateCount;
		}
	}
	
	void commit() throws SQLException {commit(loadConnection);}

	@Override
	public void close() {}

	private void resetSQL(String beforeScript, String insertStatement, String eltScript, String afterScript) throws SQLException {
		this.beforeScript = beforeScript;
		this.eltScript = eltScript;
		this.afterScript = afterScript;
		this.insertStatement = insertStatement;
		
		if(preparedStatement != null && !preparedStatement.isClosed()) preparedStatement.close();
		preparedStatement = null; 
	}
	
	private void rowStart(PreparedStatement preparedStatement) throws SQLException {
		preparedStatement.clearParameters();
		logWarnings(preparedStatement);
	}
	
	private void rowEnd(PreparedStatement preparedStatement) throws SQLException {
		preparedStatement.addBatch();
		logWarnings(preparedStatement);
	}

	/**
	 * @param connection to be committed.
	 * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
	 */
	private void commit(Connection connection) throws SQLException {connection.commit();}
	
	/**
	 * @param script with SQL statements to run against open scriptConnection.
	 * @return ResulSets, update counts and ... as JSON document.
	 * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
	 */
	private JSONArray runScript(String script) throws SQLException {
		JSONArray json = new JSONArray();
		if(script != null && !script.trim().isEmpty()) try (Statement statement = scriptConnection.createStatement()) {
			logWarnings(statement);
			executeOneScript(script, statement, json);
		}
		if(!scriptConnection.getAutoCommit()) scriptConnection.commit();
		return json;
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
	 * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
	 */
	private void logWarnings(Connection connection) throws SQLException {
		logWarnings(connection.getWarnings());
		connection.clearWarnings();
	}
	
	/**
	 * @param statement to print warnings for.
	 * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
	 */
	private void logWarnings(Statement statement) throws SQLException {
		logWarnings(statement.getWarnings());
		statement.clearWarnings();
	}
	
	/**
	 * @param resultSet to print warnings for.
	 * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
	 */
	private void logWarnings(ResultSet resultSet) throws SQLException {
		logWarnings(resultSet.getWarnings());
		resultSet.clearWarnings();
	}
	
	/**
	 * @param warning as starting point to print chain of warnings.
	 * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
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
		
		Column(ParameterMetaData parameterMetaData, int parameterIndex) throws SQLException  {
			this.targetSqlType = parameterMetaData.getParameterType(parameterIndex);
			if((this.scaleOrLength = parameterMetaData.getScale(parameterIndex)) == 0)
				this.scaleOrLength = parameterMetaData.getPrecision(parameterIndex);
		}
		
		void bind(PreparedStatement preparedStatement, int parameterIndex, Object value) throws SQLException {
			preparedStatement.setObject(parameterIndex, value, targetSqlType, scaleOrLength);
		}
	}
}
