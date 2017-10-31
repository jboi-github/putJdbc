package com.teradata.nifi.processors.teradata;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.*;

@Tags({"SQL", "load", "database", "JDBC", "bulk", "ELT", "ETL", "relational", "RDBMS", "CSV"})
@CapabilityDescription(
        "putJdbc loads bulk amounts of data into a relational database. Scripts around the " +
                "loading can be configured to setup and tear down special parameters for bulk loading as well " +
                "as issuing ELT jobs against the loaded data." +
                "Special use cases and considerations:\n" +
                "- commit, rollback and exception handling\n" +
                "    - If only one connection is given, putJdbc does not explicitly commit the database. It assumes, " +
                "that a database commit is done as part of the after loading script.\n" +
                "    - If an explicit loading connection is given, this connection is set to AutoCommit off at the " +
                "beginning and then, after execute batch, a database commit is issued, if AutoCommit is actually off. " +
                "Some databases do not understand the AutoCommit command.\n" +
                "    - The NIFI session is committed, when the after-loading-script has finished successfully. The " +
                "idea is, that the data is accessible for the user at this point in time. Before that, the data " +
                "might be loaded into a loading table, but is not visible to the user. On the other hand, if the " +
                "loading runs directly into a user accessible table, the after loading script will do most likely " +
                "only issue a database commit. In this case the NIFI commit happens directly after the database " +
                "commit.\n" +
                "    - Whenever a SQLException is thrown, it issues a rollback on all open database connections " +
                "of the instance of putJdbc and also a rollback on the NIFI session. This causes the current " +
                "FlowFile to be pushed back into the queue for rework. The processor yields unless the SQLException " +
                "indicates, to retry the command immediately.\n" +
                "    - All database Warnings and Exceptions are written to the log with the appropriate level.\n" +
                "    - Whenever an Exception is thrown during the parsing of the incoming FlowFile, it issues a " +
                "rollback on all open database connections of the instance of putJdbc. It then penalises and " +
                "transfers the FlowFile to the failure relationship.\n" +
                "- %TABLE_ID% and %COMMIT_EPOCH%\n" +
                "    - All occurrences of this text in all scripts and the insert statement are replaced by:\n" +
                "        - TABLE_ID is a 10 characters long random value consisting of the 10 digits and 26 lower " +
                "case characters. It is build once when the processor starts and before it connects to the " +
                "database. This number can have up to 3.6e15 different values and is built every time the processor " +
                "starts a thread and kept as long as there’re Flowfiles in the queue. If you would connect every " +
                "minute with 100 threads in parallel it would take about 58 Million years to have a conflict on " +
                "TABLE_ID with 50% probability. It’s seeded to the current time in nano seconds, a hash of the ip " +
                "address and the current thread id. Therefore it very likely, that each thread on each server in " +
                "a NIFI cluster has a different seed, producing a different TABLE_ID. Even with the extremely rare " +
                "cases of conflicting TABLE_ID’s it is good practise to have some code in the connected- or the " +
                "before loading script to fail, if the TABLE_ID was already used.\n" +
                "        - COMMIT_EPOCH is a count of the number of loadings happened within the current " +
                "connection. For the first loading, this number is 0, the second loading get 1 and so on. " +
                "COMMIT_EPOCH is always 0 for the connect script and contains the number of successful loadings " +
                "in the disconnecting script.\n" +
                "    - Both values are available as text so they can be easily used as placeholders in an " +
                "UpdateAttribute processor as described below. They’re also available as attributes in the " +
                "FlowFile and set as attributes in the original FlowFile and the FlowFile transferred to the " +
                "script relationship.\n" +
                "- sqlite bulk load handles larger amounts of rows best, when using a row-by-row insert, that " +
                "was prepared and is only committed once per batch.\n" +
                "    - Define one connection and set “build one batch” to “By RecordReader row-by-row”\n" +
                "    - In connected-script: Optionally set some Pragmas\n" +
                "    - In before loading script: “begin transaction”\n" +
                "    - In after loading script: “commit” and optionally before or after the commit additional ELT.\n" +
                "- Limit number of simultaneously running bulk loads. If your database has a limit on " +
                "simultaneously running bulk load jobs or you want to make sure, that your database does not get " +
                "overloaded. But still want to run ELT jobs in parallel to loading.\n" +
                "    - Set number of threads of the processor to at least two, so that one processor can run " +
                "ELT while another thread loads data.\n" +
                "    - Use an extra Connection Pool Service Controller for the loading connection\n" +
                "    - In this extra Connection Pool Service for the loading connection, limit the number of " +
                "connections to 1 and set timeout as high as one loading might need to run. You might want to " +
                "add some extra time to the timeout, just in case.\n" +
                "- Make sure FlowFiles contain a minimum number of rows as bulk loads usually profit from larger " +
                "number of rows\n" +
                "    - Place a MergeContent or MergeRecord (Version >= 1.4) processor before putJdbc to merge " +
                "FlowFile content together.\n" +
                "- Fill different tables with one processor instance. If you have limited the number of " +
                "concurrently running threads of this processor but have to fill different tables with different " +
                "preparation, ELT, insert statement etc.\n" +
                "    - Place an UpdateAttributes processor before putJdbc to set attributes for all scripts and " +
                "for the insert statement\n" +
                "    - Use the attributes in the corresponding properties of putJdbc\n" +
                "    - You can use %TABLE_ID% and %COMMIT_EPOCH% to name the loading table uniquely.\n")
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
@DynamicProperty(
        name = "record-field-name.1, record-field-name.2, ...",
        value = "Name of the field, defined in RecordReader, to be bound to Insert Statement.",
        description = "Parameter to define name of the record field to be bound to insert statement "
                + "at the corresponding position. Data type is taken from the reader schema and must match "
                + "the data type in the database table. Valid data types currently are: "
                + "Integer, Double, String, Date, Time and Timestamp (with Milliseconds granularity).")

@SeeAlso({DBCPService.class, RecordReaderFactory.class})

public class PutJdbc extends AbstractProcessor {
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
            .required(false)
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
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor BEFORE_LOADING_SCRIPT = new PropertyDescriptor.Builder()
            .name("Before Loading Script")
            .description("SQL Script to run right before loading into table starts. Most likely this script " +
                    "contains the creation of an empty table for loading The tablename can be " +
                    "constructed using %TABLE_ID% and %COMMIT_EPOCH%. If the script returns result " +
                    "sets and or update counts, they're collected in a Json document and transferred to the " +
                    "script relationship.")
            .required(false)
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
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor BEFORE_DISCONNECTING_SCRIPT = new PropertyDescriptor.Builder()
            .name("Before Disconnecting Script")
            .description("SQL Script to run right before the primary connection is closed. Most likely this " +
                    "script contains tear down statements for the session. If the script returns result " +
                    "sets and or update counts, they're collected in a Json document and transferred to the " +
                    "script relationship.")
            .required(false)
            .expressionLanguageSupported(true)
            .build();

    private static final PropertyDescriptor BUILD_BATCH_STRATEGY = new PropertyDescriptor.Builder()
            .name("Build Batch Strategy")
            .description("Defines how incoming FlowFile is interpreted and batch towards database is built.")
            .defaultValue("By RecordReader as batch")
            .allowableValues(
                    new AllowableValue(
                            "By RecordReader as batch",
                            "By RecordReader as batch",
                            "FlowFile content is read and parsed by a RecordReader. The record fields to bind " +
                                    "to the Prepared Statement are defined by dynamic properties " +
                                    "record-field-name.1 .. record-field-name.N."),
                    new AllowableValue(
                            "By RecordReader row-by-row",
                            "By RecordReader as batch",
                            "FlowFile content is read and parsed by a RecordReader. The record fields to " +
                                    "bind to the Prepared Statement are defined by dynamic properties " +
                                    "record-field-name.1 .. record-field-name.N. an execute batch is issued between " +
                                    "every row."),
                    new AllowableValue(
                            "CSV",
                            "CSV",
                            "Take FlowFile as input and pass it to Fastload-CSV capabilities of " +
                                    "Teradata’s Jdbc driver. This is only tested with Teradata. The load connection " +
                                    "is mandatory as it must be especially defined and the RecordReader must be " +
                                    "empty as it is ignored in this case. Also additional dynamic properties are " +
                                    "not allowed.")
                     )
            .required(false)
            .build();

    private static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and " +
                    "determining the data's schema. Depending on the settings of build batch strategy, this" +
                    "property must be set.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(false)
            .build();

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are passed through with additional attributes TABLE_ID and COMMIT_EPOCH")
            .autoTerminateDefault(true)
            .build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be parsed by given RecordReader. Note, that if a failure " +
                    "happens on the database, the NIFI session is rolled back an the FlowFile pushed back " +
                    "to the incoming queue.")
            .autoTerminateDefault(false)
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
            CONNECTION_POOL_PRIMARY, INSERT_STATEMENT,
            AFTER_CONNECTED_SCRIPT, BEFORE_LOADING_SCRIPT, AFTER_LOADED_SCRIPT, BEFORE_DISCONNECTING_SCRIPT,
            BUILD_BATCH_STRATEGY, CONNECTION_POOL_LOADING, RECORD_READER_FACTORY));
    private Map<String, PropertyDescriptor> supportedDynamicPropertyDescriptors = new HashMap<>();

    private Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(SUCCESS, FAILURE, SCRIPT)));

    // Parameters
    private enum BuildBatchStrategy {CSV, ROW_BY_ROW, BATCH}
    private DBCPService connectionPoolPrimary;
    private DBCPService connectionPoolLoading;
    private RecordReaderFactory recordReaderFactory;
    private BuildBatchStrategy buildBatchStrategy = BuildBatchStrategy.BATCH;
    private PropertyValue afterConnected, beforeLoading, insertStatement, afterLoaded, beforeDisconnecting;
    private String[] fieldNames;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> list = new ArrayList<>(supportedPropertyDescriptors);
        list.addAll(supportedDynamicPropertyDescriptors.values());
        return list;
    }

    @Override
    public final PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return supportedDynamicPropertyDescriptors.get(propertyDescriptorName);
    }

    @Override
    public Set<Relationship> getRelationships() {return relationships;}

    /**
     * When insert statement has changed, maintain list of supported and dynamic properties.
     * @param descriptor of the changed property.
     * @param oldValue value before change.
     * @param newValue current value after change.
     *
	 * @see org.apache.nifi.components.AbstractConfigurableComponent#onPropertyModified(org.apache.nifi.components.PropertyDescriptor, java.lang.String, java.lang.String)
	 */
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        try {
            if(BUILD_BATCH_STRATEGY.equals(descriptor)) {
                if("CSV".equals(newValue))
                    buildBatchStrategy = BuildBatchStrategy.CSV;
                else if("By RecordReader as batch".equals(newValue))
                    buildBatchStrategy = BuildBatchStrategy.BATCH;
                else if("By RecordReader row-by-row".equals(newValue))
                    buildBatchStrategy = BuildBatchStrategy.ROW_BY_ROW;
            }

            if(buildBatchStrategy == BuildBatchStrategy.CSV) {
                supportedDynamicPropertyDescriptors.clear();
            }

            if(!INSERT_STATEMENT.equals(descriptor) || buildBatchStrategy == BuildBatchStrategy.CSV) return;

            long countQuestionMarks = newValue.chars().filter(c -> c == '?').count();
            Map<String, PropertyDescriptor> supportedDynamicPropertyDescriptors = new HashMap<>();

            for(int i = 1; i <= countQuestionMarks; i++) {
                String name = "record-field-name." + i;
                PropertyDescriptor propertyDescriptor = this.supportedDynamicPropertyDescriptors.get(name);
                if(propertyDescriptor == null) {
                    propertyDescriptor = new PropertyDescriptor.Builder()
                            .name(name)
                            .displayName(name)
                            .description("Parameter to define name of the record field to be bound to insert statement "
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
        } finally {
            super.onPropertyModified(descriptor, oldValue, newValue);
        }
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext context) {
        ValidationResult nonEmptyRecordReader = StandardValidators.NON_EMPTY_VALIDATOR.validate(
                "",
                context.getProperty(RECORD_READER_FACTORY).getValue(),
                context);

        return Arrays.asList(
                (buildBatchStrategy == BuildBatchStrategy.CSV)?
                        new ValidationResult.Builder()
                                .subject(RECORD_READER_FACTORY.getDisplayName())
                                .explanation(" must be empty if build batch strategy is set to CSV.")
                                .valid(!nonEmptyRecordReader.isValid())
                                .build()
                        :
                        nonEmptyRecordReader,
                StandardValidators.NON_EMPTY_VALIDATOR.validate(
                        CONNECTION_POOL_LOADING.getDisplayName(),
                        context.getProperty(CONNECTION_POOL_LOADING).getValue(),
                        context)
                );
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getLogger().info("Correct version? 2017-10-28 23:41");

        // Get build batch strategy property
        String buildBatchStrategy = context.getProperty(BUILD_BATCH_STRATEGY).getValue();
        if("CSV".equals(buildBatchStrategy))
            this.buildBatchStrategy = BuildBatchStrategy.CSV;
        else if("By RecordReader as batch".equals(buildBatchStrategy))
            this.buildBatchStrategy = BuildBatchStrategy.BATCH;
        else if("By RecordReader row-by-row".equals(buildBatchStrategy))
            this.buildBatchStrategy = BuildBatchStrategy.ROW_BY_ROW;

        // Get connections properties
        connectionPoolPrimary = context.getProperty(CONNECTION_POOL_PRIMARY).asControllerService(DBCPService.class);
        connectionPoolLoading = context.getProperty(CONNECTION_POOL_LOADING).asControllerService(DBCPService.class);

        // Get RecordReader property
        recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        // Get script properties
        afterConnected = context.getProperty(AFTER_CONNECTED_SCRIPT);
        beforeLoading = context.getProperty(BEFORE_LOADING_SCRIPT);
        afterLoaded = context.getProperty(AFTER_LOADED_SCRIPT);
        beforeDisconnecting = context.getProperty(BEFORE_DISCONNECTING_SCRIPT);
        insertStatement = context.getProperty(INSERT_STATEMENT);

        // Get field names out of properties
        if(this.buildBatchStrategy != BuildBatchStrategy.CSV) {
            int nofColumns = (int) insertStatement.getValue().chars().filter(c -> c == '?').count();
            fieldNames = new String[nofColumns];
            for (int i = 1; i <= nofColumns; i++) {
                fieldNames[i - 1] = context.getProperty("record-field-name." + i).getValue();
            }
        }

        // Log values
        getLogger().debug("buildBatchStrategy: " + this.buildBatchStrategy);
        getLogger().debug("afterConnected: " + afterConnected);
        getLogger().debug("beforeLoading: " + beforeLoading);
        getLogger().debug("afterLoaded: " + afterLoaded);
        getLogger().debug("beforeDisconnecting: " + beforeDisconnecting);
        getLogger().debug("insertStatement: " + insertStatement);
        getLogger().debug("Record-Field-Names: " + Arrays.deepToString(fieldNames));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
