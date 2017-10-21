/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

/**
 * A parser into NIFI-Records, that can run on multiple InputStreams in parallel.
 * Each InputStream is expected to be a document in itself.
 *  
 * @author juergenb
 *
 */
public class FlexProcessor extends AbstractBaseProcessor {
	private RecordReaderFactory recordReaderFactory;
	private String[] fieldNames;
	
	public FlexProcessor(
			ProcessSession session,
			Relationship success, Relationship failure, Relationship script,
			Connection scriptConnection, Connection loadConnection,
			PropertyValue beforeScript, PropertyValue insertStatement, PropertyValue eltScript, PropertyValue afterScript,
			RecordReaderFactory recordReaderFactory, String[] fieldNames, ComponentLog logger) throws SQLException
	{
		super(session, success, failure, script, scriptConnection, loadConnection,
				beforeScript, insertStatement, eltScript, afterScript, logger);
		this.recordReaderFactory = recordReaderFactory;
		this.fieldNames = fieldNames;
	}

	/* (non-Javadoc)
	 * @see com.teradata.nifi.processors.teradata.AbstractPutJdbcHighSpeed.BaseProcessor#process(org.apache.nifi.flowfile.FlowFile)
	 */
	@Override
	public int load(InputStream in, FlowFile flowFile, JdbcWriter jdbcWriter, ComponentLog logger)
			throws SQLException, IOException, SchemaNotFoundException
	{
		int i = 0;
		try (RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, logger)) {
			for(Record record = recordReader.nextRecord(); record != null; record = recordReader.nextRecord()) {
				i++;
				jdbcWriter.addRow(parse(record));
			}
		} catch (MalformedRecordException e) {
			logger.warn(
					e.getMessage() + ((i == 0)? " -> FlowFile ignored: " : " -> Records starting at " + i + " ignored in FlowFile: ") +  
					flowFile.getAttribute(CoreAttributes.UUID.key()));
		}
		return i;
	}
	
	private Object[] parse(Record record) {
		// Get fields from record
		Object[] values = Arrays
			.asList(fieldNames)
			.stream()
			.map(fieldName -> {
				try {
					RecordFieldType recordFieldType = record
							.getSchema()
							.getDataType(fieldName)
							.orElseThrow(() -> new SchemaNotFoundException("Field \"" + fieldName + "\" not found in schema"))
							.getFieldType();
						JdbcCaster caster = jdbcCaster.get(recordFieldType);
						return (caster == null)? null : caster.cast(record, fieldName);
				} catch(SchemaNotFoundException e) {
					throw new RuntimeException(e);
				}
			})
			.collect(Collectors.toList())
			.toArray();
		return values;
	}

	@FunctionalInterface
	private interface JdbcCaster {
		public Object cast(Record record, String fieldName);
	}
	
	private static final Map<RecordFieldType, JdbcCaster> jdbcCaster = new HashMap<RecordFieldType, JdbcCaster>() {
		private static final long serialVersionUID = -6419909004656722805L;

		{
			put(RecordFieldType.STRING, (record, fieldName) -> record.getAsString(fieldName));
			put(RecordFieldType.DOUBLE, (record, fieldName) -> record.getAsDouble(fieldName));
			put(RecordFieldType.INT, (record, fieldName) -> record.getAsInt(fieldName));
			put(RecordFieldType.LONG, (record, fieldName) -> record.getAsLong(fieldName));
			put(RecordFieldType.TIMESTAMP, (record, fieldName) -> new Timestamp(record.getAsLong(fieldName)));
			put(RecordFieldType.DATE, (record, fieldName) -> {
				try {
					return new java.sql.Date(24L*3600L*1000L*record.getAsInt(fieldName));
				} catch(IllegalTypeConversionException e) {
					return new java.sql.Date(record.getAsDate(fieldName, "yyyy-MM-dd").getTime());
				}

			});
			put(RecordFieldType.TIME, (record, fieldName) -> new Time((long) record.getAsInt(fieldName)));
		}
	};
}
