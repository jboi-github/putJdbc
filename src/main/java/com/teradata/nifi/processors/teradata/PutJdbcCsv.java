package com.teradata.nifi.processors.teradata;

import org.apache.nifi.annotation.behavior.InputRequirement;
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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.*;

@Tags({"SQL", "load", "database", "JDBC", "bulk", "fastload", "ELT", "ETL", "relational", "RDBMS", "CSV", "Teradata"})
@CapabilityDescription(
        "putJdbcCsv is a specialised version of putJdbc for bulk load into Teradata using FASTLOAD-CSV protocol. " +
                "The loading connection is mandatory and must have a parameter TYPE=FASTLOADCSV in the URL. Because " +
                "this version uses the build-in CSV parser of Teradata, it des not read a RecordReader or binding " +
                "information. Some more considerations when using CSV:\n" +
                "    - Make sure the incoming FLowFile contains CSV content, that is delimited by the delimiter " +
                "defined in the URL, the fields are not quoted nor are delimiter escaped accordingly to the " +
                "requirements of the Teradata JDBC driver.\n" +
                "    - In order to prepare the data into a large CSV file (with which Teradata-Fastload-CSV works " +
                "best) you might want to setup a flow of:\n" +
                "        - Some ConvertRecord with a RecordReader that reads and parses your input and a " +
                "FreeFormRecordSetWriter that writes your data with the right delimiter, might replace the " +
                "delimiter character in string fields with any other character and does some optional other " +
                "conversions. The data then are small packages of CSV records without header\n" +
                "        - Use a MergeContent with binary concatenation to collect into some reasonable large " +
                "FlowFiles. Use Text and header field to add the column names in the loading table on top of " +
                "the FlowFile\n" +
                "        - Pass the FlowFile into putJdbcBulk. Optionally use the strategy to fill different " +
                "tables with one processor instance (see below) by adding an UpdateAttribute processor in " +
                "front of putJdbcBulk.")
@WritesAttributes({
        @WritesAttribute(
                attribute = "TABLE_ID",
                description = "TABLE_ID is a 10 characters long random value consisting of the 10 digits and 26 " +
                        "lower case characters. It is build once when the processor starts and before it connects " +
                        "to the database. This number can have up to 3.6e15 different values and is built every " +
                        "time the processor starts a thread and kept as long as there’re Flowfiles in the queue. If " +
                        "you would connect every minute with 100 threads in parallel it would take about 58 Mio " +
                        "years to have a conflict on TABLE_ID with 50% probability. It’s seeded to the current " +
                        "time in nano seconds, a hash of the ip address and the current thread id. Therefore it " +
                        "very likely, that each thread on each server in a NIFI cluster has a different seed, " +
                        "producing a different TABLE_ID. Even with the extremely rare cases of conflicting " +
                        "TABLE_ID’s It is good practise to have some code in the connected- or the before loading " +
                        "script to fail, if the TABLE_ID was already used."),
        @WritesAttribute(
                attribute = "COMMIT_EPOCH",
                description = "COMMIT_EPOCH is a count of the number of loadings happened within the current " +
                        "connection. For the first loading, this number is 0, the second loading get 1 and so on. " +
                        "COMMIT_EPOCH is always 0 for the connect script and contains the number of successful " +
                        "loadings in the disconnecting script.")
})
//@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

@SeeAlso({DBCPService.class})

public class PutJdbcCsv extends AbstractProcessor {
    private static final PropertyDescriptor CONNECTION_POOL_PRIMARY = new PropertyDescriptor.Builder()
            .name("Primary JDBC Connection Pool")
            .description("Specifies the JDBC Connection Pool to use in order to run scripts against the database. "
                    + "This connection must be set up for SQL script execution. The processor will use one " +
                    "connection per task out of this pool. The connection is established when the processor gets" +
                    "a FlowFile and is disconnected when no more FlowFiles are available for processing.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    private static final PropertyDescriptor CONNECTION_POOL_LOADING = new PropertyDescriptor.Builder()
            .name("Loading JDBC Connection Pool")
            .description("Optional JDBC Connection Pool to be used, if bulk loading needs connections with" +
                    "specialised parameters. If this connection is not set, the primary connection is used " +
                    "for loading.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    private static final PropertyDescriptor INSERT_STATEMENT = new PropertyDescriptor.Builder()
            .name("Insert Statement")
            .description("Insert statement to load into table. It contains ? to bind fields, parsed out of " +
                    "FlowFile content as Records. Fields to be bound are defined in dynamic properties named " +
                    "record-field-name.N where N is the position of the parameter in the insert statement. The " +
                    "first ? is replaced by the content of the field named by the value of property " +
                    "\"record-field-name.1\". The second ? by the field named by the value of property " +
                    "\"record-field-name.2\" and so on. The type of a field must match to the type of the " +
                    "column it loads into and given by the schema of record reader. If a field cannot be bound, " +
                    "a warning is printed to the log and the column is left unset. Specific behaviour in this case " +
                    "depends on the JDBC implementation and the table definition. The tablename can be constructed " +
                    "using %TABLE_ID% and %COMMIT_EPOCH%.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor AFTER_CONNECTED_SCRIPT = new PropertyDescriptor.Builder()
            .name("After Connected Script")
            .description("SQL Script to run right after the primary connection is established. Most likely this " +
                    "script contains settings for session parameter. If the script returns result " +
                    "sets and or update counts, they're collected in a Json document and transferred to the " +
                    "script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    private static final PropertyDescriptor BEFORE_LOADING_SCRIPT = new PropertyDescriptor.Builder()
            .name("Before Loading Script")
            .description("SQL Script to run right before loading into table starts. Most likely this script " +
                    "contains the creation of an empty table for loading The tablename can be " +
                    "constructed using %TABLE_ID% and %COMMIT_EPOCH%. If the script returns result " +
                    "sets and or update counts, they're collected in a Json document and transferred to the " +
                    "script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor AFTER_LOADED_SCRIPT = new PropertyDescriptor.Builder()
            .name("After Loaded Script")
            .description("SQL Script to run right after loading into table did commit. Most likely this script " +
                    "contains ELT jobs to merge the just loaded rows into core tables. The tablename can be " +
                    "constructed using %TABLE_ID% and %COMMIT_EPOCH%. The ELT script either has code to identify " +
                    "what data was loaded or deletes the loaded rows after ELT Job. If the script returns result " +
                    "sets and or update counts, they're collected in a Json document and transferred to the " +
                    "script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor BEFORE_DISCONNECTING_SCRIPT = new PropertyDescriptor.Builder()
            .name("Before Disconnecting Script")
            .description("SQL Script to run right before the primary connection is closed. Most likely this " +
                    "script contains tear down statements for the session. If the script returns result " +
                    "sets and or update counts, they're collected in a Json document and transferred to the " +
                    "script relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are passed through with additional attributes TABLE_ID and COMMIT_EPOCH")
            .autoTerminateDefault(true)
            .build();

    private static final Relationship SCRIPT = new Relationship.Builder()
            .name("script")
            .description("After connected-script, disconnecting-script and each after-loading-script a FlowFile " +
                    "with timings, result sets and update counts are written as Json-Document.\n" +
                    "The outgoing script contains timing information (Elapsed times) and TABLE_ID, " +
                    "COMMIT_EPOCH plus result sets and update counts returned by the scripts and insert statement. " +
                    "The information given back from SQL statements is taken as is.\n" +
                    "It is created when the connecting script returns, after each loading sequence " +
                    "(after loading script finished) and right after the disconnecting script returns. If any of " +
                    "the scripts are empty, the FlowFile is still produced but will be empty.")
            .autoTerminateDefault(false)
            .build();

    private List<PropertyDescriptor> supportedPropertyDescriptors = Collections.unmodifiableList(Arrays.asList(
            CONNECTION_POOL_PRIMARY, CONNECTION_POOL_LOADING, INSERT_STATEMENT,
            AFTER_CONNECTED_SCRIPT, BEFORE_LOADING_SCRIPT, AFTER_LOADED_SCRIPT, BEFORE_DISCONNECTING_SCRIPT));

    private Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(SUCCESS, SCRIPT)));

    // Parameters
    private DBCPService connectionPoolPrimary;
    private DBCPService connectionPoolLoading;
    private PropertyValue afterConnected, beforeLoading, insertStatement, afterLoaded, beforeDisconnecting;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {return supportedPropertyDescriptors;}

    @Override
    public Set<Relationship> getRelationships() {return relationships;}

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getLogger().info("Correct version? CSV 2017-10-28 23:41");

        // Get connections properties
        connectionPoolPrimary = context.getProperty(CONNECTION_POOL_PRIMARY).asControllerService(DBCPService.class);
        connectionPoolLoading = context.getProperty(CONNECTION_POOL_LOADING).asControllerService(DBCPService.class);

        // Get script properties
        afterConnected = context.getProperty(AFTER_CONNECTED_SCRIPT);
        beforeLoading = context.getProperty(BEFORE_LOADING_SCRIPT);
        afterLoaded = context.getProperty(AFTER_LOADED_SCRIPT);
        beforeDisconnecting = context.getProperty(BEFORE_DISCONNECTING_SCRIPT);
        insertStatement = context.getProperty(INSERT_STATEMENT);

        // Log values
        getLogger().debug("afterConnected: " + afterConnected);
        getLogger().debug("beforeLoading: " + beforeLoading);
        getLogger().debug("afterLoaded: " + afterLoaded);
        getLogger().debug("beforeDisconnecting: " + beforeDisconnecting);
        getLogger().debug("insertStatement: " + insertStatement);
    }

    /**
     * @param context this processor is in.
     * @param session of this processor thread.
     * @throws ProcessException to be thrown, when anything weird happens.
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        getLogger().info("Started: " + session.getQueueSize());

        AdditionalAttributes attributes = new AdditionalAttributes(getLogger(), 10);

        try (PutContentCsv putContentCsv = new PutContentCsv(
                connectionPoolPrimary,
                afterConnected.getValue(), beforeDisconnecting.getValue(),
                attributes, getLogger()))
        {
            getLogger().info("Connected");
            for(FlowFile flowFile = session.get(); flowFile != null; flowFile = session.get()) {
                getLogger().debug("Processing FlowFile: {}MB", new Object[] {flowFile.getSize() / (1024.0 * 1024.0)});

                session.putAllAttributes(flowFile, attributes.getAsMap());
                FlowFile scriptFlowFile;
                try(InputStream in = session.read(flowFile)) {
                    scriptFlowFile = fromString(session, putContentCsv.process(
                            in, flowFile.getSize(), connectionPoolLoading,
                            attributes.evaluate(beforeLoading.evaluateAttributeExpressions(flowFile).getValue()),
                            attributes.evaluate(insertStatement.evaluateAttributeExpressions(flowFile).getValue()),
                            attributes.evaluate(afterLoaded.evaluateAttributeExpressions(flowFile).getValue())),
                            flowFile);
                }
                session.transfer(scriptFlowFile, SCRIPT);
                session.transfer(flowFile, SUCCESS);
                attributes.incEpoch();

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

    /**
     * React on thrown exception.
     *
     * @param t exception to inspect.
     * @param context where the exception happened
     * @throws ProcessException to rethrow given exception
     */
    private void react(Throwable t, ProcessContext context) throws ProcessException {
        boolean shouldYield = true;

        // Walk through chain of exceptions
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

        // Closing database connection without explicit commit does roll back when auto commit is false.
        throw new ProcessException(t); // Does a roll back of NIFI Session
    }

    /**
     * @param session of this process.
     * @param content to be packed into FlowFile
     * @param parent FlowFile containing attributes and parameters
     * @return newly constructed FLowFile with content.
     */
    private FlowFile fromString(ProcessSession session, JSONObject content, FlowFile parent) {
        return session.importFrom(
                new ByteArrayInputStream(content.toString(4).getBytes(StandardCharsets.UTF_8)),
                session.create(parent));
    }
}
