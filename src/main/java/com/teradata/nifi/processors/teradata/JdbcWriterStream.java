/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.nifi.logging.ComponentLog;

/**
 * Write stream of rows into database. Data is transfered in batches, given by size and committed 
 * either after time-interval or after a number or rows. Whatever happens first.
 * A callback is called after every commit and can be used to synchronize database- and internal commits.
 * 
 * The interface is multi-thread enabled. It ensures, that JDBC's single-thread constraint is fulfilled.
 * It is ensured, that no rows are loaded while one of the scripts is running.
 * 
 * @author juergenb
 *
 */
public class JdbcWriterStream extends Async implements AutoCloseable {
	@FunctionalInterface
	public interface OnCommit{public void committed();}
	public interface OnScript{public void scriptDone(String result);}
	
	private JdbcWriter jdbcWriter;
	private int rowsPerBatch;
	private int rowsTillCommit;
	private long msTillCommit;
	private OnCommit onCommit;
	private OnScript onScript;
	private ComponentLog logger;
	
	private int rowsInBatch = 0;
	private int rowsSinceCommit = 0;
	private long msNextCommit = Long.MAX_VALUE; // Set after begin script
	private boolean closed = false;

	public JdbcWriterStream(
			Connection scriptConnection, Connection loadConnection,
			String beforeScript, String insertStatement, String eltScript, String afterScript,
			int rowsPerBatch, int rowsTillCommit, long msTillCommit,
			OnCommit onCommit, OnScript onScript,
			ComponentLog logger) throws SQLException
	{
		super(Integer.MAX_VALUE); // Single Threaded
		jdbcWriter = new JdbcWriter(scriptConnection, loadConnection, beforeScript, insertStatement, eltScript, afterScript, logger);
		
		this.rowsPerBatch = rowsPerBatch;
		this.rowsTillCommit = rowsTillCommit;
		this.msTillCommit = msTillCommit;
		this.onCommit = onCommit;
		this.onScript = onScript;
		this.logger = logger;

		// Start with before script
		submit(() -> {
			runBeforeScript();
			msNextCommit = System.currentTimeMillis() + this.msTillCommit;
			return null;
		});
	}

	/**
	 * Puts insertion of row into queue.
	 * @param values in type and order of corresponding parameter. A null is passed by setting the object to null.
	 * @throws SQLException
	 * @see com.teradata.nifi.processors.teradata.JdbcWriter#addRow(java.lang.Object[])
	 */
	public void addRow(Object[] values) throws SQLException {
		if(closed) {
			throw new IllegalStateException("Trying to add a after writer was closed!");
		}
		
		submit(() -> {
			addRowSync(values);
			
			if(rowsSinceCommit >= rowsTillCommit || msNextCommit <= System.currentTimeMillis()) {
				commit(); // does transferBatch first
			} else if(rowsInBatch >= rowsPerBatch) {
				transferBatch();
			}
			return null;
		});
	}
	
	private void addRowSync(Object[] values) throws SQLException {
		jdbcWriter.addRow(values);
		rowsInBatch++;
		rowsSinceCommit++;
	}
	
	private void transferBatch() throws SQLException {
		logger.debug("calling transferBatch");
		jdbcWriter.transferBatch();
		rowsInBatch = 0;
		logger.debug("transferBatch done");
	}
	
	private void commit() throws SQLException {
		logger.debug("calling commit");
		transferBatch();
		
		jdbcWriter.commit();
		rowsSinceCommit = 0;
		msNextCommit = System.currentTimeMillis() + msTillCommit;
		
		onScript.scriptDone(jdbcWriter.runEltScript()); // Loading is stopped while ELT runs
		onCommit.committed();
		
		logger.debug("commit done");
	}
	
	private void runBeforeScript()  throws SQLException {
		logger.debug("calling beforeScript");
		onScript.scriptDone(jdbcWriter.runBeforeScript());
		logger.debug("beforeScript done");
	}
	
	private void runAfterScript()  throws SQLException {
		logger.debug("calling afterScript");
		onScript.scriptDone(jdbcWriter.runAfterScript());
		logger.debug("afterScript done");
	}

	@Override
	public void close() throws SQLException {
		closed = true; // Ensure, that the following submit is the last one
		logger.debug("closing");
		
		submit(() -> {
			commit();
			runAfterScript();
			jdbcWriter.close();
			return null;
		});
		
		try {
			syncAndEnd();
		} catch (InterruptedException e) {
			logger.error("Interrupted!", e);
			interrupt();
		} catch (ExecutionException e) {
			if(e.getCause() instanceof SQLException) throw (SQLException) e.getCause();
			logger.error("Houston!!!", e); // This cannot happen, or?
		}
		logger.debug("synced and closed");
	}
}
