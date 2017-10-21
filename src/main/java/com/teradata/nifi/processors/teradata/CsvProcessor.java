/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

/**
 * Easy and well integrated processor for Fastload of CSV FlowFiles.
 * Takes one FlowFile as input (must be CSV with all requirements defined by Teradata's CSV fastload)
 * and sends it to Teradata.
 * 
 * @author juergenb
 *
 */
public class CsvProcessor extends AbstractBaseProcessor {
	public CsvProcessor(
			ProcessSession session,
			Relationship success, Relationship failure, Relationship script,
			Connection scriptConnection, Connection loadConnection,
			PropertyValue beforeScript, PropertyValue insertStatement, PropertyValue eltScript, PropertyValue afterScript,
			ComponentLog logger) throws SQLException
	{
		super(session, success, failure, script, scriptConnection, loadConnection,
				beforeScript, insertStatement, eltScript, afterScript, logger);
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
	public int load(InputStream in, FlowFile flowFile, JdbcWriter jdbcWriter, ComponentLog logger) throws SQLException {
		return jdbcWriter.transferCsv(in);
	}
}
