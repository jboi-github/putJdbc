/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * Aggregates one numeric time-series value into seconds, minutes and hour buckets.
 * Values are sum, count, sum^2
 * 
 * Calls to process can be handled asynchronously but single-threaded. Callbacks will be single-threaded, one-by-one.
 * 
 * Key-fields can be of any hashable type, the value field is expected to be a double.
 * Decision to inform callback about a second, minute or hour aggregate is based on incoming data, not on elapsed time.
 * You must ensure, that this is running periodically.
 * 
 * @author juergenb
 *
 */
public class Aggregator {
	String[] keysFieldNames;
	String timestampFieldName;
	String statusFieldName;
	String valueFieldName;
	
	// Two leveled: Hash for Keys and sorted list for time-stamp. It is expected to have only few time-stamps
	Map<Key, SingleAggregation> aggsSeconds = new ConcurrentHashMap<>();
	Map<Key, SingleAggregation> aggsMinutes = new ConcurrentHashMap<>();
	Map<Key, SingleAggregation> aggsHours = new ConcurrentHashMap<>();
	
	public Aggregator(String[] keysFieldNames, String timestampFieldName, String valueFieldName, String statusFieldName) {
		this.keysFieldNames = keysFieldNames;
		this.timestampFieldName = timestampFieldName;
		this.valueFieldName = valueFieldName;
		this.statusFieldName = statusFieldName;
	}

	// Multi-Threaded
	public void process(Record record, ComponentLog logger) {
		// Get value -> Debug-Info and ignore, if not existing
		if(record.getValue(valueFieldName) == null) {
			logger.debug("field " + valueFieldName + " not found in record. Record ignored");
			return;
		}
		double value = record.getAsDouble(valueFieldName);
		
		// Build key -> Warn and ignore, if not existing and return
		Object[] keys = new Object[keysFieldNames.length];
		for(int i=0; i<keysFieldNames.length; i++) {
			if(record.getValue(keysFieldNames[i]) == null) {
				logger.warn("field " + keysFieldNames[i] + " not found in record. Record ignored");
				return;
			} else {
				keys[i] = record.getValue(keysFieldNames[i]);
			}
		}
		
		// Get time-stamp -> Warn and ignore, if not existing and return
		if(record.getValue(timestampFieldName) == null) {
			logger.warn("field " + timestampFieldName + " not found in record. Record ignored");
			return;
		}
		long timestamp = record.getAsLong(timestampFieldName);
		
		// Get status -> Warn and ignore, if not existing and return
		if(record.getValue(statusFieldName) == null) {
			logger.debug("field " + statusFieldName + " not found in record. Record ignored");
			return;
		}
		long status = record.getAsLong(statusFieldName);
		
		processRecord(keys, timestamp, value, status);
	}
	
	private void processRecord(Object[] keyFields, long timestamp, double value, long status) {
		processRecordAggregate(aggsSeconds, keyFields, ((long) (timestamp / 1000L)) * 1000L, value, status);
		processRecordAggregate(aggsMinutes, keyFields, ((long) (timestamp / 60000L)) * 60000L, value, status);
		processRecordAggregate(aggsHours, keyFields, ((long) (timestamp / 3600000L)) * 3600000L, value, status);
	}
	
	private void processRecordAggregate(Map<Key, SingleAggregation> map, Object[] keyFields, long timestamp, double value, long status) {
		map.merge(
				new Key(keyFields, timestamp),
				new SingleAggregation(value, status),
				(k, v) -> {v.process(value, status); return v;}
		);
	}
	
	// Single-Threaded
	public void transfer(
			ProcessSession session, Relationship seconds, Relationship minutes, Relationship hours,
			RecordSetWriterFactory writerFactory, ComponentLog logger) throws IOException, SchemaNotFoundException
	{
		transferRelationship(session, seconds, aggsSeconds, writerFactory, logger);
		transferRelationship(session, minutes, aggsMinutes, writerFactory, logger);
		transferRelationship(session, hours, aggsHours, writerFactory, logger);
	}
	
	private void transferRelationship(
			ProcessSession session, Relationship relationship, Map<Key, SingleAggregation> aggs,
			RecordSetWriterFactory writerFactory, ComponentLog logger) throws IOException, SchemaNotFoundException
	{
		WriteResult writeResult = null;
		String mimeType = null;
		
		FlowFile flowFile = session.create();
		RecordSchema schema = writerFactory.getSchema(null, null);
		try(		OutputStream os = session.write(flowFile);
				RecordSetWriter writer = writerFactory.createWriter(logger, schema, flowFile, os))
		{
			writer.beginRecordSet();
			
			for(Entry<Key, SingleAggregation> entry : aggs.entrySet()) {
				Map<String, Object> values = new HashMap<>();
				for(int i=0; i<keysFieldNames.length; i++) {
					values.put(keysFieldNames[i], entry.getKey().keys[i]);
				}
				values.put(timestampFieldName, entry.getKey().timestamp);
				values.put(valueFieldName + "Sum", entry.getValue().sum);
				values.put(valueFieldName + "SumSq", entry.getValue().sumSq);
				values.put(valueFieldName + "Count", entry.getValue().cntOkStatus);
				values.put(statusFieldName + "CountNotOk", entry.getValue().cntNotOkStatus);
				values.put(statusFieldName + "Min", entry.getValue().minStatus);
				values.put(statusFieldName + "Max", entry.getValue().maxStatus);
				
				writer.write(new MapRecord(schema, values));
			}
			
			writeResult = writer.finishRecordSet();
			mimeType = writer.getMimeType();
		}
		
		if(writeResult != null) {
			flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(writeResult.getRecordCount()));
			flowFile = session.putAllAttributes(flowFile, writeResult.getAttributes());
		}
		if(mimeType != null) {
			flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType);
		}
		session.transfer(flowFile, relationship);
		aggs.clear();
	}
	
	private class Key {
		public Object[] keys;
		public long timestamp;
		
		public Key(Object[] keys, long timestamp) {
			this.keys = keys;
			this.timestamp = timestamp;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + Arrays.hashCode(keys);
			result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (!(obj instanceof Key)) return false;
			
			Key other = (Key) obj;
			if (!getOuterType().equals(other.getOuterType())) return false;
			if (!Arrays.equals(keys, other.keys)) return false;
			if (timestamp != other.timestamp) return false;
			
			return true;
		}
		private Aggregator getOuterType() {
			return Aggregator.this;
		}
	}
	
	private class SingleAggregation {
		public double sum = 0.0, sumSq = 0.0;
		public long cntOkStatus = 0, cntNotOkStatus = 0, minStatus = Long.MAX_VALUE, maxStatus = Long.MIN_VALUE;
		
		public SingleAggregation(double value, long status) {
			process(value, status);
		}
		
		public void process(double value, long status) {
			if(status == 0L) {
				cntOkStatus++;
				sum += value;
				sumSq += value * value;
			} else {
				cntNotOkStatus++;
			}
			if(status < minStatus) minStatus = status;
			if(status > maxStatus) maxStatus = status;
		}
	}
}
