package com.teradata.nifi.processors.teradata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.teradata.nifi.processors.teradata.AbstractPutJdbc.BaseProcessor;

/**
 * @author juergenb
 *
 */
public abstract class AbstractBaseProcessor implements BaseProcessor {
	private ProcessSession session;
	private Relationship success, failure, script;
	private Connection scriptConnection, loadConnection;
	private PropertyValue beforeScript, insertStatement, eltScript, afterScript;
	private ComponentLog logger;
	
	private AdditionalAttributes atts;

	AbstractBaseProcessor(
			ProcessSession session,
			Relationship success, Relationship failure, Relationship script,
			Connection scriptConnection, Connection loadConnection,
			PropertyValue beforeScript, PropertyValue insertStatement, PropertyValue eltScript, PropertyValue afterScript,
			ComponentLog logger) throws SQLException
	{
		this.session = session;
		this.success = success;
		this.failure = failure;
		this.script = script;
		this.scriptConnection = scriptConnection;
		this.loadConnection = loadConnection;
		this.beforeScript = beforeScript;
		this.insertStatement = insertStatement;
		this.eltScript = eltScript;
		this.afterScript = afterScript;
		this.logger = logger;
		
		atts = new AdditionalAttributes(logger, 10);
	}

	/* (non-Javadoc)
	 * @see java.lang.AutoCloseable#close()
	 */
	@Override
	public void close() {}

	/* (non-Javadoc)
	 * @see com.teradata.nifi.processors.teradata.AbstractPutJdbcHighSpeed.BaseProcessor#process(org.apache.nifi.flowfile.FlowFile)
	 */
	@Override
	public void process(FlowFile flowFile) throws Exception {
		long startTimestamp = System.currentTimeMillis();
		
		JSONObject result = new JSONObject();
		result.put("start", startTimestamp);
		result.put("TABLE_ID", atts.getTableId());
		result.put("COMMIT_EPOCH", atts.getCommitEpoch());
		result.put("node", InetAddress.getLocalHost());
		result.put("threadId", Thread.currentThread().getId());
		
		boolean successfully = false;
		try(JdbcWriter jdbcWriter = new JdbcWriter(
				scriptConnection, loadConnection,
				atts.evaluate(beforeScript, flowFile), atts.evaluate(insertStatement, flowFile),
				atts.evaluate(eltScript, flowFile), atts.evaluate(afterScript, flowFile),
				logger)) {
			
			result.put("before", decorate(jdbcWriter::runBeforeScript));
			try (InputStream in = session.read(flowFile)) {
				long loadStart = System.currentTimeMillis();
				int loadRows = load(in, flowFile, jdbcWriter, logger);
				jdbcWriter.commit();
				long loadStop = System.currentTimeMillis();
				
				JSONObject load = new JSONObject();
				load.put("start", loadStart);
				load.put("stop", loadStop);
				load.put("elapsed", loadStop - loadStart);
				load.put("rows", loadRows);
				result.put("load", load);
			}
			result.put("ELT", decorate(jdbcWriter::runEltScript));
			result.put("after", decorate(jdbcWriter::runAfterScript));

			successfully = true;
		} finally {
			session.transfer(flowFile, (successfully)? success:failure);
			
			long stopTimestamp = System.currentTimeMillis();
			result.put("stop", stopTimestamp);
			result.put("elapsed", stopTimestamp - startTimestamp);
			session.transfer(fromString(result.toString(), flowFile), script);
			
			if(successfully) {
				atts.incEpoch();
				session.commit();
			}
		}
	}
	
	protected abstract int load(InputStream in, FlowFile flowFile, JdbcWriter jdbcWriter, ComponentLog logger) throws SQLException, IOException, SchemaNotFoundException;
    
	@FunctionalInterface
	private interface ScriptRunner {
		JSONArray run() throws SQLException;
	}
	
	private JSONObject decorate(ScriptRunner scriptRunner) throws JSONException, SQLException {
		long startTimestamp = System.currentTimeMillis();
		
		JSONObject result = new JSONObject();
		result.put("start", startTimestamp);
		result.put("result", scriptRunner.run());
		
		long stopTimestamp = System.currentTimeMillis();
		result.put("stop", stopTimestamp);
		result.put("elapsed", stopTimestamp - startTimestamp);
		
		return result;
	}
	
    private FlowFile fromString(String result, FlowFile flowFile) {
    		return session.importFrom(
    				new ByteArrayInputStream(result.getBytes(StandardCharsets.UTF_8)),
    				session.create(flowFile));
	}
}
