package com.teradata.nifi.processors.teradata;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

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
@DynamicProperty(
        name = "record-field-name.1, record-field-name.2, ...",
        value = "Name of the field, defined in RecordReader, to be bound to Insert Statement.",
        description = "Parameter to define name of the record field to be bound to insert statement "
                + "at the corresponding position. Data type is taken from the reader schema and must match "
                + "the data type in the database table. Valid data types currently are: "
                + "Integer, Double, String, Date, Time and Timestamp (with Milliseconds granularity).")
@SeeAlso({DBCPService.class, RecordReaderFactory.class})
public class PutJdbc extends AbstractPutJdbc {
    private static final PropertyDescriptor CONNECTION_POOL_LOADING = CONNECTION_POOL_LOADING_BUILDER
            .required(false)
            .build();

    private static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and " +
                    "determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    private static final PropertyDescriptor BUILD_BATCH_STRATEGY = new PropertyDescriptor.Builder()
            .name("Build Batch Strategy")
            .description("Defines how incoming FlowFile is interpreted and batches towards database are built.")
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
                            "By RecordReader row-by-row",
                            "FlowFile content is read and parsed by a RecordReader. The record fields to " +
                                    "bind to the Prepared Statement are defined by dynamic properties " +
                                    "record-field-name.1 .. record-field-name.N. An executeBatch is issued between " +
                                    "every row.")
                     )
            .required(true)
            .build();

    private static final PropertyDescriptor.Builder RECORD_FIELD_BUILDER = new PropertyDescriptor.Builder()
            .description("Parameter to define name of the record field to be bound to insert statement "
                    + "at the corresponding position. Data type is taken from the schema and must match "
                    + "the data type in the database table. Valid data types currently are: "
                    + "Integer, Double, String, Date, Time and Timestamp (with Milliseconds granularity).")
            .dynamic(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false);

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be parsed by given RecordReader. Note, that if a failure " +
                    "happens on the database, the NIFI session is rolled back an the FlowFile pushed back " +
                    "to the incoming queue.")
            .autoTerminateDefault(false)
            .build();

    // Parameters
    private RecordReaderFactory recordReaderFactory;
    private boolean sendEachRow;
    private String[] fieldNames;

    private Map<String, PropertyDescriptor> supportedDynamicPropertyDescriptors = new HashMap<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propertyDescriptors = super.getSupportedPropertyDescriptors();
        propertyDescriptors.add(1, CONNECTION_POOL_LOADING);
        propertyDescriptors.add(2, RECORD_READER_FACTORY);
        propertyDescriptors.add(BUILD_BATCH_STRATEGY);
        propertyDescriptors.addAll(supportedDynamicPropertyDescriptors.values());
        return propertyDescriptors;
    }

    @Override
    public final PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return supportedDynamicPropertyDescriptors.get(propertyDescriptorName);
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = super.getRelationships();
        relationships.add(FAILURE);
        return relationships;
    }

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
        super.onPropertyModified(descriptor, oldValue, newValue);

        if(!getInsertStatement().equals(descriptor)) return;

        long countQuestionMarks = newValue.chars().filter(c -> c == '?').count();
        Map<String, PropertyDescriptor> supportedDynamicPropertyDescriptors = new HashMap<>();

        for(int i = 1; i <= countQuestionMarks; i++) {
            String name = "record-field-name." + i;
            PropertyDescriptor propertyDescriptor = this.supportedDynamicPropertyDescriptors.get(name);
            if(propertyDescriptor == null) {
                propertyDescriptor = RECORD_FIELD_BUILDER
                        .name(name)
                        .displayName(name)
                        .defaultValue(name)
                        .build();
            }
            supportedDynamicPropertyDescriptors.put(name, propertyDescriptor);
        }
        this.supportedDynamicPropertyDescriptors = supportedDynamicPropertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context, CONNECTION_POOL_LOADING);

        // Get build batch strategy property
        sendEachRow = "By RecordReader row-by-row".equals(context.getProperty(BUILD_BATCH_STRATEGY).getValue());

        // Get RecordReader property
        recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        // Get field names out of properties
        int nofColumns = (int) context.getProperty(AbstractPutJdbc.getInsertStatement())
                .getValue()
                .chars()
                .filter(c -> c == '?')
                .count();
        fieldNames = new String[nofColumns];
        for (int i = 1; i <= nofColumns; i++) {
            fieldNames[i - 1] = context.getProperty("record-field-name." + i).getValue();
        }

        // Log values
        getLogger().debug("sendEachRow: " + sendEachRow);
        getLogger().debug("Record-Field-Names: " + Arrays.deepToString(fieldNames));
    }

    /**
     * Load one FlowFile using the prepared statement. Call after the before-loading-script has finished
     * The statement is prepared on the right connection as defined in the connection pool properties.
     * When done, the processor will call the after-loading-script.
     *
     * @param session   the process runs
     * @param flowFile  to be processed
     * @param statement as it was prepared from property
     * @param logger    to print messages to log
     * @return update count, e.g. number of rows loaded.
     * @throws IOException  occurred when reading from FlowFile
     * @throws SQLException occurred when loading to database.
     */
    @Override
    protected int load(ProcessSession session, FlowFile flowFile, PreparedStatement statement, ComponentLog logger) throws IOException, SQLException {
        int rows = 0;
        try (InputStream inputStream = session.read(flowFile);
             RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, inputStream, logger))
        {
            RecordCaster[] recordCasters = buildRecordCasters(recordReader.getSchema(), fieldNames);
            JdbcBinder[] jdbcBinders = buildJdbcBinders(statement.getParameterMetaData());

            for(Record record = recordReader.nextRecord(); record != null; record = recordReader.nextRecord()) {
                rows++;

                // Set parameters of insert statement. Do not clear but keep previous parameters, if current record lacks field
                for (int i = 0; i < fieldNames.length; i++) {
                    if(recordCasters[i] == null) continue; // Try with previous parameter binding
                    jdbcBinders[i].bind(statement, i+1, recordCasters[i].cast(record, fieldNames[i]));
                }
                statement.addBatch();
                if(sendEachRow) statement.executeBatch();
            }
            if(rows > 0 && !sendEachRow) statement.executeBatch();
        } catch (MalformedRecordException e) {
            logger.warn("{} in Record {} of FlowFile. FlowFile transferred to failure.",
                    new Object[] {e.getMessage(), rows});
        } catch (SchemaNotFoundException e) {
            logger.warn("{} FlowFile transferred to failure.", new Object[] {e.getMessage()});
        }
        return rows;
    }

    /*
     * Material to get all necessary casts from record to sql data type in the right order for fast processing
     */

    /**
     * @param schema of records.
     * @param fieldNames in records to be bound to corresponding parameter.
     * @return arrays of RecordCaster in order of field names.
     */
    private RecordCaster[] buildRecordCasters(RecordSchema schema, String[] fieldNames) {
        return Arrays
                .stream(fieldNames)
                .map(fieldName -> schema.getDataType(fieldName).orElse(null))
                .map(dataType -> (dataType == null)? null:dataType.getFieldType())
                .map(fieldType -> (fieldType == null)? null:recordCasters.get(fieldType))
                .collect(Collectors.toList()).toArray(new RecordCaster[] {});
    }

    /**
     * Make initialization easier
     */
    @FunctionalInterface
    private interface RecordCaster {
        Object cast(Record record, String fieldName);
    }

    /**
     * Possible and supported RecordCaster.
     */
    private static final Map<RecordFieldType, RecordCaster> recordCasters = new HashMap<RecordFieldType, RecordCaster>() {
        private static final long serialVersionUID = -6419909004656722805L;

        {
            put(RecordFieldType.STRING, Record::getAsString);
            put(RecordFieldType.DOUBLE, Record::getAsDouble);
            put(RecordFieldType.INT, Record::getAsInt);
            put(RecordFieldType.LONG, Record::getAsLong);
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

    /*
     * Material to get all necessary bindings for parameter in prepared statement
     */

    /**
     * @param parameterMetaData meta data of prepared statement.
     * @return Necessary binders in order of parameters in prepared statement.
     */
    private JdbcBinder[] buildJdbcBinders(ParameterMetaData parameterMetaData) throws SQLException {
        JdbcBinder[] jdbcBinders = new JdbcBinder[parameterMetaData.getParameterCount()];
        for(int i = 0; i < jdbcBinders.length; i++) {
            jdbcBinders[i] = new JdbcBinder(parameterMetaData, i+1);
        }
        return jdbcBinders;
    }

    /**
     * Binding of generic Object to a parameter.
     */
    private class JdbcBinder {
        private int targetSqlType;
        private int scaleOrLength;

        JdbcBinder(ParameterMetaData parameterMetaData, int parameterIndex) throws SQLException  {
            this.targetSqlType = parameterMetaData.getParameterType(parameterIndex);
            if((this.scaleOrLength = parameterMetaData.getScale(parameterIndex)) == 0)
                this.scaleOrLength = parameterMetaData.getPrecision(parameterIndex);
        }

        void bind(PreparedStatement preparedStatement, int parameterIndex, Object value) throws SQLException {
            preparedStatement.setObject(parameterIndex, value, targetSqlType, scaleOrLength);
        }
    }
}
