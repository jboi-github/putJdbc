/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.teradata.nifi.processors.teradata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

/**
 * A Nifi Processor to load into Teradata via JDBC in the fastest possible (probably at fastload-speed) way.
 * It follows the pattern of an Data Egress Processor as described here:<br>
 * <a>https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html#data-egress</a><br><br>
 * <quote>A Processor that publishes data to an external source has two Relationships: success and failure.
 * The Processor name starts with "Put" followed by the protocol that is used for data transmission.</quote>
 * 
 * @author juergenb
 */
@Tags({"Teradata", "SQL", "load", "database", "JDBC", "bulk", "fastload", "ELT", "relational", "RDBMS"})
@CapabilityDescription(
		"Load bulk amount of data into Teradata possibly using Fastload capabilities. "
		+ "Define an insert statement for a table to land the data and have this processor start scripts for mini-batched ELT. "
		+ "The Loader uses a landing tables to load into. It first runs a script before it loads into the table. When the script "
		+ "has finished load of table starts. After a configurable time and amount of data this processor runs the after script "
		+ "and commits its work. Any exception that happens will roll back and yield the processor if it was not a transient error. "
		+ "The processor works best, if the before script ensures an empty and exclusive table to load the data. Thus, together with "
		+ "the commit and roll back for a session in Nifi ensures, that Flow Files are always loaded once and only once into the "
		+ "database.")
@WritesAttributes({
	@WritesAttribute(
			attribute = "TABLE_ID",
			description = "A 10 characters long random value ontaining only digits (0-9) and "
				+ "lowercase letters (a-z). This attribute can be used to construct a table "
				+ "name which will be unique in the far most cases.")
	})
//@TriggerSerially
@InputRequirement(Requirement.INPUT_REQUIRED)
@DynamicProperty(
		name = "record-field-name.1, record-field-name.2, ...",
		value = "Name of the field, defined in Record Reader, to be bound to Prepared Statement.",
		description = "Parameter to define name of the record field to be bound to prepared statement "
				+ "at the corresponding position. Data type is taken from the schema and must match "
				+ "the data type in the database table. Valid data types currently are: "
				+ "Integer, Double, String, Date, Time and Timestamp (with Milliseconds granularity).")
@SeeAlso({DBCPService.class})
public class PutJdbcHighSpeed extends AbstractProcessor {
	private static final PropertyDescriptor CONNECTION_POOL_SCRIPT = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool for Before- and After Script")
            .description("Specifies the JDBC Connection Pool to use in order to run Before- and After Scripts. "
            		+ "This connection must be set up for SQL script execution. "
            		+ "The processor will use one connection per task out of this pool.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    private static final PropertyDescriptor CONNECTION_POOL_LOAD = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool for high speed loading")
            .description("Specifies the JDBC Connection Pool to use in order to load into landing table. "
            		+ "This connection must be set up for high speed data loading, most likely with parameter TYPE=FASTLOAD set in the URL. "
            		+ "The processor will use one connection per task out of this pool.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();
    
    private static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader defining input schema")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    
    private static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("Aggregates Record Writer")
            .displayName("Record Writer defining output schema for aggregates")
            .description("Specifies the Controller Service to use for writing outgoing aggregates on second, minute and hour level. "
            		+ "The schema must define all keys defined in \"Aggregate Keys\", \"Aggregate Timestamp\", "
            		+ "\"Aggregate Value\" extended by \"Sum\" (as double), \"SumSq\" (as double), \"Count\" (as long), "
            		+ "\"Aggregate Status\" extended by \"CountNotOk\" (as long), \"Min\" (as long), \"Max\" (as long)")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(false)
            .build();

   private static final PropertyDescriptor BEFORE_SCRIPT = new PropertyDescriptor.Builder()
            .name("Before Script")
            .displayName("Before Script for Landing Table")
            .description("SQL Script to run before loading to landing table starts. "
            		+ "It is guaranteed, that the before script did finish before loading starts. "
             	+ "Most likely this script will (re)create the landing table or delete all rows in it. "
	        		+ "The tablename can be constructed using \"prefix_${TABLE_ID}_postfix\". "
	        		+ "If the Script returns result sets and or update counts, they're collected in a Json document "
	        		+ "and transfered to the script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor ELT_SCRIPT = new PropertyDescriptor.Builder()
            .name("ELT Script")
            .displayName("ELT Script for Landing Table")
            .description("SQL Script to run right after loading to landing table did finish. "
            		+ "It is guaranteed, that the loading did finish before the ELT script starts. "
            		+ "Most likely this script contains ELT jobs to merge the just landed rows into core tables. "
            		+ "The tablename can be constructed using \"prefix_${TABLE_ID}_postfix\". "
            		+ "If the Script returns result sets and or update counts, they're collected in a Json document "
            		+ "and transfered to the script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor AFTER_SCRIPT = new PropertyDescriptor.Builder()
            .name("After Script")
            .displayName("After Script for Landing Table")
            .description("SQL Script to run after loading to landing table and ELT script did finish. "
            		+ "It is guaranteed, that the loading and the ELT script did finish before the after script starts. "
            		+ "Most likely this script contains clean up jobs to drop or empty the landed table. "
            		+ "The tablename can be constructed using \"prefix_${TABLE_ID}_postfix\". "
            		+ "If the Script returns result sets and or update counts, they're collected in a Json document "
            		+ "and transfered to the script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor PREPARED_STATEMENT = new PropertyDescriptor.Builder()
            .name("Prepared Statement")
            .displayName("Prepared Statement for loading into Landing Table")
            .description("Insert statement to load into table. It contains ? to bind attributes. "
            		+ "Attributes to be bound are named by their position within the prepared statement. "
            		+ "The first ? is replaced by the content of the attribute named \"value.1\" in the FlowFile. "
            		+ "The second ? by \"value.2\" and so on. The type of an Attribute must match to the type of the "
            		+ "column it loads into and given by the property \"type.1\", \"type.1\" etc. If an attribue cannot "
            		+ "be bound, a warning is printed to the log and the column is left unset. Specific behaviour in this case "
            		+ "depends on the JDBC implementation. "
            		+ "The tablename can be constructed using \"prefix_${TABLE_ID}_postfix\". ")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    
    private static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Flow File Batch Size")
            .displayName("Batch size of Flow Files transfered to the database in one chunk")
            .description("Batch size defines how much FlowFiles are packed together "
            		+ "to be sent to the database. Default is 10000. While this is an important tuning parameter consider, that on one hand "
            		+ "larger batches mean less communication with the server and therefore much higher throuput. On the "
            		+ "other hand are the batched FlowFiles kept in memory and consume JVM's heap space. "
            		+ "Also make sure, that system limits of the database are not violated. The given batch size is an arbitrary number. "
            		+ "Actual batch size varies from 1 row up to a theoretical limit of (2 * batch size -1) rows that are"
            		+ "transfered within one batch. Note also, that each FlowFile can produce a number of rows, depending on the "
            		+ "Record Reader configuration.")
            .required(false)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor ROWS_PER_LOAD = new PropertyDescriptor.Builder()
            .name("Rows per load")
            .displayName("Rows loaded before committed")
            .description("Number of records to be loaded as rows into landing table "
            		+ "before the load finishes and the after script starts. Default is 100000 rows. "
            		+ "Switching might happen earlier, if TIME_PER_LOAD was exceeded first. "
            		+ "Also can the number of rows loaded in one hop be larger, if there were more Flow Files in the queue. "
            		+ "The given rows per load is an arbitrary number. Actual rows vary from 1 row up to a theoretical "
            		+ "limit of (roundup(rows per load / batch size) * batch size + batch size -1) rows that are "
            		+ "transfered within one batch. Whwre batch size is acutally the defined (batch size x rows in a Flow File).")
            .required(false)
            .defaultValue("100000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor TIME_PER_LOAD = new PropertyDescriptor.Builder()
            .name("Time per load")
            .displayName("Time elapsed in load before committed")
            .description("Maximum number of Seconds elapsed before the after script starts. "
            		+ "Default is 10 seconds. Switching might happen earlier, if ROWS_PER_LOAD was exceeded first.")
            .required(false)
            .defaultValue("10 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character set for SQL")
            .displayName("Character set to use for SQL commands")
            .description("Some databases do not support full UTF-8 character sets. In this "
            		+ "case try to limit down the characters in use to ASCII or Latin. Note that "
            		+ "characters, that can't be converted are replaced by ?. "
            		+ "Check the log on debug level for the effectively tansfered SQL's")
            .defaultValue(Charset.defaultCharset().name())
			.allowableValues(Charset.availableCharsets().keySet().toArray(new String[0]))
			.expressionLanguageSupported(false)
			.required(false)
            .build();

    private static final PropertyDescriptor AGGREGATE = new PropertyDescriptor.Builder()
            .name("Aggregate")
            .displayName("Build aggregates on seconds, minutes and hours level")
            .description("Do the aggregation. IF false all other aggregation properties are ignored.")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
			.expressionLanguageSupported(false)
			.required(true)
            .build();

    private static final PropertyDescriptor AGGREGATE_KEYS = new PropertyDescriptor.Builder()
            .name("Aggregate Keys")
            .displayName("Aggregate keys")
            .description("Record fields making the keys, that splt the aggregation. "
            		+ "The keys are a comma seperated list of record filed names.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(false)
			.required(false)
            .build();

    private static final PropertyDescriptor AGGREGATE_TIMESTAMP = new PropertyDescriptor.Builder()
            .name("Aggregate Timestamp")
            .displayName("Aggregate Timestamp")
            .description("Record field making the timestamp.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(false)
			.required(false)
            .build();

    private static final PropertyDescriptor AGGREGATE_VALUE = new PropertyDescriptor.Builder()
            .name("Aggregate Value")
            .displayName("Aggregate Value")
            .description("Record field making the value.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(false)
			.required(false)
            .build();

    private static final PropertyDescriptor AGGREGATE_STATUS = new PropertyDescriptor.Builder()
            .name("Aggregate Status")
            .displayName("Aggregate Status")
            .description("Record field making the status.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(false)
			.required(false)
            .build();

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All succesfully executed data is passed to this relationship. "
            		+ "This processor always syncs commits and roll backs between the database and the Nifi session. "
            		+ "Therefore, flow files are either passed to success when the data is comitted in the database or "
            		+ "the whole session and database transaction is rolled back, possibly yielding the processor and "
            		+ "flow files are pushed back to the input queue as part of the Nifi-session-roll-back.")
            .autoTerminateDefault(true)
            .build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All unsuccesfully parsed data is passed to this relationship. "
            		+ "This processor always syncs commits and roll backs between the database and the Nifi session. "
            		+ "Therefore, flow files are only passed to failure, when the data had a format or parsing issue. "
            		+ "These flow files are then transfered to failure and non of its records is send to the database.")
            .autoTerminateDefault(false)
            .build();

    private static final Relationship SCRIPT = new Relationship.Builder()
            .name("script")
            .description("Results from scripts can be one or more result sets "
            		+ "and update counts. All of them are packed in a json document "
            		+ "that is passed with the flow file.")
            .autoTerminateDefault(false)
            .build();

    private static final Relationship SECOND = new Relationship.Builder()
            .name("aggregate.seconds")
            .description("Aggregation on second level. Only used, when Property \"Aggregate\" is true.")
            .autoTerminateDefault(true)
            .build();

    private static final Relationship MINUTE = new Relationship.Builder()
            .name("aggregate.minutes")
            .description("Aggregation on minute level. Only used, when Property \"Aggregate\" is true.")
            .autoTerminateDefault(true)
            .build();

    private static final Relationship HOUR = new Relationship.Builder()
            .name("aggregate.hours")
            .description("Aggregation on hour level. Only used, when Property \"Aggregate\" is true.")
            .autoTerminateDefault(true)
            .build();

    private List<PropertyDescriptor> supportedPropertyDescriptors;
    private Map<String, PropertyDescriptor> supportedDynamicPropertyDescriptors;
    private Set<Relationship> relationships;
    
    private int batchSize; // Parameter
    private int rowsPerLoad; // Parameter
    private long msPerLoad; // Parameter
    private String[] fieldNames; // Parameter
    private int nofColumns; // Parameter
    private PropertyValue beforeScript, eltScript, afterScript, preparedStatement; // Parameter
    private PropertyValue charsetName; // Parameter
    
    private boolean aggregate; // If false ignore rest
    private String[] aggKeys = null;
    private String aggTimestamp = null;
    private String aggValue = null;
    private String aggStatus = null;
		 
    @Override
    protected void init(final ProcessorInitializationContext context) {
		getLogger().debug("init!");
		
        this.supportedPropertyDescriptors = Collections.unmodifiableList(Arrays.asList(
        		CONNECTION_POOL_SCRIPT, CONNECTION_POOL_LOAD, RECORD_READER_FACTORY,
            	PREPARED_STATEMENT, BEFORE_SCRIPT, ELT_SCRIPT, AFTER_SCRIPT,
            	BATCH_SIZE, ROWS_PER_LOAD, TIME_PER_LOAD, CHARSET,
            	AGGREGATE, RECORD_WRITER_FACTORY, AGGREGATE_KEYS, AGGREGATE_TIMESTAMP, AGGREGATE_VALUE, AGGREGATE_STATUS));
        this.supportedDynamicPropertyDescriptors = new HashMap<String, PropertyDescriptor>(); // Empty and modifiable
        this.relationships = Collections.unmodifiableSet(
        		new HashSet<Relationship>(Arrays.asList(SUCCESS, FAILURE, SCRIPT, SECOND, MINUTE, HOUR)));
    }

    @Override
    public Set<Relationship> getRelationships() {return this.relationships;}

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		List<PropertyDescriptor> list = new ArrayList<PropertyDescriptor>(supportedPropertyDescriptors);
		list.addAll(supportedDynamicPropertyDescriptors.values());
		return list;
    }

    @Override
    public final PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
    		return supportedDynamicPropertyDescriptors.get(propertyDescriptorName);
	}
    
    /* (non-Javadoc)
     * When prepared statement has changed, maintain list of supported, dynamic and required properties.
	 * @see org.apache.nifi.components.AbstractConfigurableComponent#onPropertyModified(org.apache.nifi.components.PropertyDescriptor, java.lang.String, java.lang.String)
	 */
	@Override
	public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
		if(PREPARED_STATEMENT.equals(descriptor)) {
			long countQuestionMarks = newValue.chars().filter(c -> c == '?').count();
			Map<String, PropertyDescriptor> supportedDynamicPropertyDescriptors = new HashMap<String, PropertyDescriptor>();
			
			for(int i = 1; i <= countQuestionMarks; i++) {
				String name = "record-field-name." + i;
				PropertyDescriptor propertyDescriptor = this.supportedDynamicPropertyDescriptors.get(name);
				if(propertyDescriptor == null) {
					propertyDescriptor = new PropertyDescriptor.Builder()
							.name(name)
							.displayName(name)
							.description("Parameter to define name of the record field to be bound to prepared statement "
									+ "at the corresponding position. Data type is taken from the schema and must match "
									+ "the data type in the database table. Valid data types currently are: "
									+ "Integer, Double, String, Date, Time and Timestamp (with Milliseconds granularity).")
							.defaultValue(name)
							.dynamic(true)
							.expressionLanguageSupported(false)
							.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
							.required(false)
							.build();
				}
				supportedDynamicPropertyDescriptors.put(name, propertyDescriptor);
			}
			this.supportedDynamicPropertyDescriptors = supportedDynamicPropertyDescriptors;
		}
		super.onPropertyModified(descriptor, oldValue, newValue);
	}

	/**
     * This Processor creates or initializes a Connection Pool in the method that uses the @OnScheduled annotation.
     * However, because communications problems may prevent connections from being established or cause connections
     * to be terminated, connections themselves are not created at this point. Rather, the connections are created
     * or leased from the pool in the onTrigger method.
     * 
     * @param context
	 * @throws IOException 
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
		getLogger().info("Correct version? 2017-10-06 09:41");
		getLogger().debug("onScheduled!");

       	// Get static properties
		batchSize = context.getProperty(BATCH_SIZE).asInteger();
		rowsPerLoad = context.getProperty(ROWS_PER_LOAD).asInteger();
		msPerLoad = context.getProperty(TIME_PER_LOAD).asTimePeriod(TimeUnit.MILLISECONDS);
		nofColumns = (int) context.getProperty(PREPARED_STATEMENT).getValue().chars().filter(c -> c == '?').count();
		beforeScript = context.getProperty(BEFORE_SCRIPT);
		eltScript = context.getProperty(ELT_SCRIPT);
		afterScript = context.getProperty(AFTER_SCRIPT);
		preparedStatement = context.getProperty(PREPARED_STATEMENT);
		charsetName = context.getProperty(CHARSET);
		
		// Get field names out of properties
		fieldNames = new String[nofColumns];
		for(int i = 1; i <= nofColumns; i++) {
			fieldNames[i-1] = context.getProperty("record-field-name." + i).getValue();
		}
		
		// Aggregation properties
		aggregate = context.getProperty(AGGREGATE).asBoolean();
		if(aggregate) {
			aggKeys = Arrays
					.asList(context.getProperty(AGGREGATE_KEYS).getValue().split(","))
					.stream()
					.map(s -> s.trim())
					.collect(Collectors.toList())
					.toArray(new String[0]);
			aggTimestamp = context.getProperty(AGGREGATE_TIMESTAMP).getValue();
			aggValue = context.getProperty(AGGREGATE_VALUE).getValue();
			aggStatus = context.getProperty(AGGREGATE_STATUS).getValue();
		}
		
		getLogger().debug(TIME_PER_LOAD.getName() + ": " + msPerLoad);
    		getLogger().debug(ROWS_PER_LOAD.getName() + ": " + rowsPerLoad);
    		getLogger().debug(BATCH_SIZE.getName() + ": " + batchSize);
    		getLogger().debug(CHARSET.getName() + ": " + charsetName);
    		getLogger().debug("Record-Field-Names: " + Arrays.deepToString(fieldNames));
    		getLogger().debug(PREPARED_STATEMENT.getName() + ": " + preparedStatement.getValue());
        getLogger().debug(BEFORE_SCRIPT.getName() + ": " + beforeScript.getValue());
        getLogger().debug(ELT_SCRIPT.getName() + ": " + eltScript.getValue());
        getLogger().debug(AFTER_SCRIPT.getName() + ": " + afterScript.getValue());

        getLogger().debug(AGGREGATE.getName() + ": " + aggregate);
        if(aggregate) {
        		getLogger().debug(AGGREGATE_KEYS.getName() + ": " + Arrays.deepToString(aggKeys));
            getLogger().debug(AGGREGATE_TIMESTAMP.getName() + ": " + aggTimestamp);
            getLogger().debug(AGGREGATE_VALUE.getName() + ": " + aggValue);
            getLogger().debug(AGGREGATE_STATUS.getName() + ": " + aggStatus);
        }
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
 		getLogger().debug("onTrigger! " + session.getQueueSize());
 		
		// Get a random number to use in scripts and prepared statement.
       	String shortUUID = generateShortUUID(10);
       	getLogger().debug("TABLE_ID -> " + shortUUID);

       	// Initialize Record Reader Factory
		RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

		// Aggregations
		final RecordSetWriterFactory recordWriterFactory = (aggregate)? context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class):null;
		final Aggregator aggregator = (aggregate)? new Aggregator(aggKeys, aggTimestamp, aggValue, aggStatus):null;
		
		// JdbcWriter is AutoClosable. When it closes it automatically rolls back everything, that was not explicitly committed
		try (JdbcWriterStream jdbcWriterStream = new JdbcWriterStream(
				context.getProperty(CONNECTION_POOL_SCRIPT).asControllerService(DBCPService.class).getConnection(),
				context.getProperty(CONNECTION_POOL_LOAD).asControllerService(DBCPService.class).getConnection(),
				evaluate(beforeScript, shortUUID),
				evaluate(preparedStatement, shortUUID),
				evaluate(eltScript, shortUUID),
				evaluate(afterScript, shortUUID),
				batchSize, rowsPerLoad, msPerLoad,
				() -> aggregate(session, aggregator, recordWriterFactory),
				(result) -> session.transfer(fromString(result, session), SCRIPT),
				getLogger());)
		{
			getLogger().debug("onTrigger: connections created");
			
			NifiStreamUtil
				.asStream(session)
				.map(flowFile -> {
					try {
						return NifiStreamUtil.readContent(session, flowFile);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				})
				.flatMap(flowFileContent -> {
					try {
						return NifiStreamUtil.asStream(
								recordReaderFactory, flowFileContent.getFlowFile(), 
								flowFileContent.getInputStream(), getLogger());
					} catch (IOException | SchemaNotFoundException e) {
						throw new RuntimeException(e);
					}
				})
				.forEach(record -> {
					try {
						NifiStreamUtil.addRow(record, fieldNames, jdbcWriterStream);
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
					if(aggregate) aggregator.process(record, getLogger());
				});
		} catch (UnsupportedEncodingException | SQLException | RuntimeException e) {
			getLogger().debug("onTrigger: Exception!");
			react((e instanceof RuntimeException)? e.getCause() : e, context);
		}
		getLogger().debug("OnTrigger: Ended with empty Queue");
    }

    private void aggregate(ProcessSession session, Aggregator aggregator, RecordSetWriterFactory recordWriterFactory) {
		if(aggregate) try {
			aggregator.transfer(session, SECOND, MINUTE, HOUR, recordWriterFactory, getLogger());
		} catch (IOException | SchemaNotFoundException e) {
			getLogger().error("Aggregation-Transfer problem - no aggregates built", e);
		}
    }
    
    private FlowFile fromString(String s, ProcessSession session) {
    		return session.importFrom(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)),session.create());
    }
    
    private void react(Throwable e, ProcessContext context) throws ProcessException {
		// What are the detailed errors on SQL? First Exception will be printed by abstract processor
		if(e instanceof SQLException)
			for(SQLException sqlEx = ((SQLException) e).getNextException(); sqlEx != null; sqlEx = sqlEx.getNextException()) {
				getLogger().error(sqlEx.getMessage(), sqlEx);
			}
		
		// Retry immediately?
		if(!(e instanceof SQLTransientException)) context.yield();
		
		// Closing database connection without explicit commit does roll back because auto commit is false.
		throw new ProcessException(e); // Does a roll back of NIFI Session
    }

	private final static char[] ALPHABET = {
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 
			'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 
			'u', 'v', 'w', 'x', 'y', 'z'};

    private String generateShortUUID(int length) {
    		StringBuilder sb = new StringBuilder(length);
    		Random random = new Random();
    		
    		try {
			random.setSeed(InetAddress.getLocalHost().getHostAddress().hashCode() + Thread.currentThread().getId() + System.nanoTime());
		} catch (UnknownHostException e) {
			random.setSeed(System.nanoTime());
		}
   		for(int i = 0; i < length; i++) sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
    		return sb.toString();
	}
    
    private String evaluate(PropertyValue propertyValue, String tableId) throws UnsupportedEncodingException {
    		String property = propertyValue.evaluateAttributeExpressions(Collections.singletonMap("TABLE_ID", tableId)).getValue();
    		if(property != null) property = new String(property.getBytes(Charset.forName(charsetName.getValue())), charsetName.getValue());
    		
    		getLogger().debug("Effectively set to: \"" + property + "\", coming from \"" + propertyValue.getValue() + "\"");
    		return property;
    }
}
