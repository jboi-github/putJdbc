/**
 * 
 */
package com.teradata.nifi.processors.teradata;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Common Processor behavior for all putJdbcHighSpeed Processors
 * 
 * @author juergenb
 *
 */
public abstract class AbstractPutJdbc extends AbstractProcessor {
    protected static final PropertyDescriptor CONNECTION_POOL_SCRIPT = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool for Before-, ELT- and After Script")
            .description("Specifies the JDBC Connection Pool to use in order to run Before-, ELT- and After-Scripts. "
            		+ "This connection must be set up for SQL script execution. The processor will use one connection per "
            		+ "task out of this pool.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    protected static final PropertyDescriptor CONNECTION_POOL_LOAD = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool for high speed loading")
            .description("Specifies the JDBC Connection Pool to use in order to load into landing table. "
            		+ "This connection must be set up for high speed data loading, most likely with parameter TYPE=FASTLOAD "
            		+ "set in the URL. The processor will use one connection per task out of this pool.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    protected static final PropertyDescriptor BEFORE_SCRIPT = new PropertyDescriptor.Builder()
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

    protected static final PropertyDescriptor ELT_SCRIPT = new PropertyDescriptor.Builder()
            .name("ELT Script")
            .displayName("ELT Script for Landing Table")
            .description("SQL Script to run right after loading to landing table did commit. "
            		+ "It is guaranteed, that the loading did commit before the ELT script starts and loading is "
            		+ "paused while the ELT Script runs. Most likely this script contains ELT jobs to merge the "
            		+ "just landed rows into core tables. The tablename can be constructed using "
            		+ "\"prefix_${TABLE_ID}_postfix\". The ELT script either has code to identify what data was "
            		+ "loaded since last commit or deletes the loaded rows after ELT Job. If the Script returns "
            		+ "result sets and or update counts, they're collected in a Json document and transfered to "
            		+ "the script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected static final PropertyDescriptor AFTER_SCRIPT = new PropertyDescriptor.Builder()
            .name("After Script")
            .displayName("After Script for Landing Table")
            .description("SQL Script to run after loading to landing table and the processor did finish. "
            		+ "It is guaranteed, that the loading and the ELT script was committed and did finish before "
            		+ "the after script starts. Most likely this script contains clean up jobs to drop the landed table. "
            		+ "The tablename can be constructed using \"prefix_${TABLE_ID}_postfix\". "
            		+ "If the Script returns result sets and or update counts, they're collected in a Json document "
            		+ "and transfered to the script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected static final PropertyDescriptor PREPARED_STATEMENT = new PropertyDescriptor.Builder()
            .name("Prepared Statement")
            .displayName("Prepared Statement for loading into Landing Table")
            .description("Insert statement to load into table. It contains ? to bind fields, parsed out of FlowFile content. "
            		+ "Fields to be bound are defined in dynamic parameters namend named record-field-name.N where N is the position of the parameter."
            		+ "The first ? is replaced by the content of the field named by the value of parameter \"record-field-name.1\"."
            		+ "The second ? by the field named by the value of parameter \"record-field-name.2\" and so on. "
            		+ "The type of an field must match to the type of the column it loads into and given by the schema of record reader."
            		+ "If a field cannot be bound, a warning is printed to the log and the column is left unset. "
            		+ "Specific behaviour in this case depends on the JDBC implementation. "
            		+ "The tablename can be constructed using \"prefix_${TABLE_ID}_postfix\". ")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character set for SQL")
            .displayName("Character set to use for SQL commands")
            .description("Some databases do not support full UTF-8 character sets. In this "
            		+ "case try to limit down the characters in use to ASCII or Latin. Note that "
            		+ "characters, that can't be converted are replaced by ?. "
            		+ "Check the log on info level for the effectively tansfered SQL's")
            .defaultValue(Charset.defaultCharset().name())
			.allowableValues(Charset.availableCharsets().keySet().toArray(new String[0]))
			.expressionLanguageSupported(false)
			.required(false)
            .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All succesfully executed data is passed to this relationship. "
            		+ "This processor always syncs commits and roll backs between the database and the Nifi session. "
            		+ "Therefore, flow files are either passed to success when the data is comitted in the database or "
            		+ "the whole session and database transaction is rolled back, possibly yielding the processor and "
            		+ "flow files are pushed back to the input queue as part of the Nifi-session-roll-back.")
            .autoTerminateDefault(true)
            .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All unsuccesfully parsed data is passed to this relationship. "
            		+ "This processor always syncs commits and roll backs between the database and the Nifi session. "
            		+ "Therefore, flow files are only passed to failure, when the data had a format or parsing issue. "
            		+ "These flow files are then transfered to failure and non of its records is send to the database.")
            .autoTerminateDefault(false)
            .build();

    protected static final Relationship SCRIPT = new Relationship.Builder()
            .name("script")
            .description("Results from scripts can be one or more result sets "
            		+ "and update counts. All of them are packed in a json document "
            		+ "that is passed with the flow file.")
            .autoTerminateDefault(false)
            .build();
    
    /**
     * Interface for a processor. To be initialized by child classes.
     * A processor is initialized by overwriting createProcessor method.
     *
     * @author juergenb
     */
    public interface BaseProcessor extends AutoCloseable {
    		public void process(FlowFile flowFile) throws Exception;
    }
    
    private List<PropertyDescriptor> supportedPropertyDescriptors;
    private Set<Relationship> relationships;

    private PropertyValue beforeScript, eltScript, afterScript, preparedStatement; // Parameter
    private PropertyValue charsetName; // Parameter

    @Override
    protected void init(final ProcessorInitializationContext context) {
		getLogger().debug("init!");
		
        supportedPropertyDescriptors = new ArrayList<>();
        supportedPropertyDescriptors.addAll(Arrays.asList(new PropertyDescriptor[] {
        		CONNECTION_POOL_SCRIPT, CONNECTION_POOL_LOAD,
            	PREPARED_STATEMENT, BEFORE_SCRIPT, ELT_SCRIPT, AFTER_SCRIPT, CHARSET}));
        this.relationships = Collections.unmodifiableSet(new HashSet<Relationship>(Arrays.asList(SUCCESS, FAILURE, SCRIPT)));
    }

    @Override
    public Set<Relationship> getRelationships() {return this.relationships;}

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		List<PropertyDescriptor> list = new ArrayList<PropertyDescriptor>(supportedPropertyDescriptors);
		return list;
    }
    
    /**
     * @param context
     * @throws IOException
     */
    public void onScheduled(final ProcessContext context) {
       	// Get static properties
		beforeScript = context.getProperty(BEFORE_SCRIPT);
		eltScript = context.getProperty(ELT_SCRIPT);
		afterScript = context.getProperty(AFTER_SCRIPT);
		preparedStatement = context.getProperty(PREPARED_STATEMENT);
		charsetName = context.getProperty(CHARSET);
		
		getLogger().debug(PREPARED_STATEMENT.getName() + ": " + preparedStatement.getValue());
        getLogger().debug(BEFORE_SCRIPT.getName() + ": " + beforeScript.getValue());
        getLogger().debug(ELT_SCRIPT.getName() + ": " + eltScript.getValue());
        getLogger().debug(AFTER_SCRIPT.getName() + ": " + afterScript.getValue());
		getLogger().debug(CHARSET.getName() + ": " + charsetName);
    }		

	/* (non-Javadoc)
	 * @see org.apache.nifi.processor.AbstractProcessor#onTrigger(org.apache.nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
	 */
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
 		getLogger().info("Started: " + session.getQueueSize());
 		
 		DBCPService dbcpServiceScript = context.getProperty(CONNECTION_POOL_SCRIPT).asControllerService(DBCPService.class);
		DBCPService dbcpServiceLoad = context.getProperty(CONNECTION_POOL_LOAD).asControllerService(DBCPService.class);
		
		// Parser with JdbcWriter is AutoClosable. When it closes it automatically rolls back everything, that was not explicitly committed
		try (	Connection scriptConnection =  dbcpServiceScript.getConnection();
				Connection loadConnection =  dbcpServiceLoad.getConnection();
				BaseProcessor processor = createProcessor(
						context, session, SUCCESS, FAILURE, SCRIPT,
						scriptConnection, loadConnection,
						charConversion(beforeScript), charConversion(preparedStatement),
						charConversion(eltScript), charConversion(afterScript), getLogger()))
		{
 	 		getLogger().info("Connected");
			for(FlowFile flowFile = session.get(); flowFile != null; flowFile = session.get()) {
				getLogger().debug("Processing FlowFile: " + flowFile.getSize()/(1024.0 * 1024.0) + "MB");
	 	 		processor.process(flowFile);
				getLogger().debug("Processed.");
	 	 		
	 	 		if(!isScheduled()) {
	 	 			getLogger().info("Stopped by user");
	 	 			break;
	 	 		}
	 		}
		} catch (Exception e) {
			getLogger().error("Exception - Processing stopped");
			react(e, context);
		}
		getLogger().info("Done");
	}
	
	protected abstract BaseProcessor createProcessor(
			ProcessContext context, ProcessSession session,
			Relationship success, Relationship failure, Relationship script,
			Connection scriptConnection, Connection loadConnection,
			PropertyValue beforeScript, PropertyValue preparedStatement, PropertyValue eltScript, PropertyValue afterScript,
			ComponentLog logger) throws SQLException;
	
    private void react(Throwable t, ProcessContext context) throws ProcessException {
    		boolean shouldYield = true;
    		
    		// Walk thru chain of exceptions
    		for(Throwable e = t; e != null; e = e.getCause()) {
			// What are the detailed errors on SQL?
			if(e instanceof SQLException)
				for(SQLException sqlEx = (SQLException) e; sqlEx != null; sqlEx = sqlEx.getNextException()) {
					getLogger().error(sqlEx.getMessage(), sqlEx);
				}
			
			// Retry immediately?
			if(e instanceof SQLTransientException) shouldYield = false;
    		}
    		
    		if(shouldYield) context.yield();
    		
		// Closing database connection without explicit commit does roll back because auto commit is false.
		throw new ProcessException(t); // Does a roll back of NIFI Session
    }
    
    private PropertyValue charConversion(PropertyValue propertyValue) throws UnsupportedEncodingException {
		String property = new String(propertyValue.getValue().getBytes(Charset.forName(charsetName.getValue())), charsetName.getValue());
		getLogger().info("Effectively used as: \"" + property + "\" coming from: \"" + propertyValue.getValue() + "\"");
		
		return propertyValue;
    }
}
