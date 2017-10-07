/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

/**
 * Collection of functions and interfaces.
 * 
 * @author juergenb
 *
 */
public class NifiStreamUtil {
	/**
	 * Create a lazy stream of FlowFiles from session. Stream ends when no more FlowFiles waiting in any incoming Q.
	 * If more incoming Q's are connected to the processor, the Stream continues till all Qs are empty.
	 * 
	 * @param session to get FlowFile from.
	 * @return a Stream of FlowFiles for further processing.
	 */
	public static Stream<FlowFile> asStream(ProcessSession session) {
		Spliterator<FlowFile> spliterator = Spliterators.spliteratorUnknownSize(new Iterator<FlowFile>() {
				@Override
				public boolean hasNext() {return session.getQueueSize().getObjectCount() > 0;}
	
				@Override
				public FlowFile next() {return session.get();}
			}, 0);
		return StreamSupport.stream(spliterator, false).filter(ff -> ff != null);
	}
	
	/**
	 * Reading content from FlowFiles is not thread safe as of version 1.3. Reading content of FlowFiles from one session in
	 * multiple threads leads to IllegalStateException's. Therefore this method reads all content of one Flowfile into memory
	 * and wraps it into an InputStream for further processing like parsing into Records.
	 * 
	 * @param flowFile to get content from.
	 * @return an InputStream backed by a byte-array that can be used in multi-threaded environments.
	 * @throws IOException when reading the FlowFile encounters an error. This is a major issue.
	 */
	public static FlowFileContent readContent(ProcessSession session, FlowFile flowFile) throws IOException {
		byte[] content = new byte[(int) flowFile.getSize()];
		try (InputStream in = session.read(flowFile)) {
			int off = 0;
			int len;
			while((len = in.read(content, off, content.length - off)) >= 0) off += len;
		} // autocloseable closes here
		
		return new FlowFileContent(flowFile, new ByteArrayInputStream(content));
	}
	
	public static class FlowFileContent {
		private FlowFile flowFile;
		private InputStream inputStream;
		
		public FlowFileContent(FlowFile flowFile, InputStream inputStream) {
			this.flowFile = flowFile;
			this.inputStream = inputStream;
		}

		public FlowFile getFlowFile() {return flowFile;}
		public InputStream getInputStream() {return inputStream;}
	}
	
	/**
	 * Creates a stream of records out of a FlowFile and its content.
	 * 
	 * @param recordReaderFactory for the parser of incoming content.
	 * @param flowFile with attributes, necessary to configure parser.
	 * @param inputStream with content to parse into records.
	 * @param logger to write messages, if anything happens.
	 * 
	 * @return Stream of records. When an error occurs, this stream might be empty.
	 * 
	 * @throws IOException major issue with NIFI.
	 * @throws SchemaNotFoundException something wrong with the parser configuration.
	 */
	public static Stream<Record> asStream(
			RecordReaderFactory recordReaderFactory,
			FlowFile flowFile, InputStream inputStream,
			ComponentLog logger) throws IOException, SchemaNotFoundException
	{
		try {
			RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, inputStream, logger);
			Spliterator<Record> spliterator = Spliterators.spliteratorUnknownSize(new Iterator<Record>() {
				private Record record = null;
				
				@Override
				public boolean hasNext() {
					try {
						record = recordReader.nextRecord();
					} catch (IOException | MalformedRecordException e) {
						throw new RuntimeException(e);
					}
					return record != null;
				}
	
				@Override
				public Record next() {return record;}
			}, 0);
			return StreamSupport.stream(spliterator, true);
		} catch (MalformedRecordException e) {
			logger.warn(e.getMessage() + " -> FlowFile ignored: " + flowFile.getAttribute(CoreAttributes.UUID.key()));
			return StreamSupport.stream(Spliterators.emptySpliterator(), true);
		}
	}
	
	/**
	 * Put one record into database and submit to JdbcWriterStream.
	 * 
	 * @param record to to put into database.
	 * @param jdbcWriterStream to write to database.
	 * @param fieldNames expected in record where position in array corresponds to index of parameter.
	 * 		fieldNames[0] = Parameter 1, fieldNames[1] = Parameter 2, ...
	 * @param record2JdbcBinderMap map with code to get field out of record and map to expected data type of parameter.
	 * @throws SQLException 
	 */
	public static void addRow(Record record, String[] fieldNames, JdbcWriterStream jdbcWriterStream) throws SQLException {
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
		jdbcWriterStream.addRow(values);
	}
	
	@FunctionalInterface
	private interface JdbcCaster {
		public Object cast(Record record, String fieldName);
	}
	
	static final Map<RecordFieldType, JdbcCaster> jdbcCaster = new HashMap<RecordFieldType, JdbcCaster>() {
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
