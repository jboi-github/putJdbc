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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
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
		+ "has finished load of table starts. After a configurable time and amount of data this processor runs the ELT script "
		+ "and commits its work. When th Queue gets empty or the USer stops this processor, a final commit-sequence (database-commit, "
		+ "ELT Script, Nifi-commit) runs and the after script is started. Any exception that happens will roll back and yield the "
		+ "processor if it was not a transient error. The processor works best, if the before script ensures an empty and exclusive "
		+ "table to load the data. Thus, together with the commit and roll back for a session in Nifi ensures, that Flow Files "
		+ "are always loaded once and only once into the database.")
@WritesAttributes({
	@WritesAttribute(
			attribute = "TABLE_ID",
			description = "A 10 characters long random value ontaining only digits (0-9) and "
				+ "lowercase letters (a-z). This attribute can be used to construct a table "
				+ "name which will be unique in the far most cases."),
	@WritesAttribute(
			attribute = "COMMIT_EPOCHE",
			description = "An integer counting how often this task locally ran thru the commit sequence. "
					+ "The number can be loaded into the load table and helpd to identify in the ELT which "
					+ "rows where loaded in latest batch.")
	})
//@TriggerSerially
@InputRequirement(Requirement.INPUT_REQUIRED)
@DynamicProperty(
		name = "record-field-name.1, record-field-name.2, ...",
		value = "Name of the field, defined in Record Reader, to be bound to Prepared Statement.",
		description = "Parameter to define name of the record field to be bound to prepared statement "
				+ "at the corresponding position. Data type is taken from the reader schema and must match "
				+ "the data type in the database table. Valid data types currently are: "
				+ "Integer, Double, String, Date, Time and Timestamp (with Milliseconds granularity).")
@SeeAlso({DBCPService.class, RecordReaderFactory.class, RecordSetWriterFactory.class})
public class PutJdbcFlex extends AbstractPutJdbc {
    private static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader defining input schema")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    private Map<String, PropertyDescriptor> supportedDynamicPropertyDescriptors;
    
    private String[] fieldNames; // Parameter
    private int nofColumns; // Parameter

    @Override
    protected void init(final ProcessorInitializationContext context) {
    		super.init(context);
        this.supportedDynamicPropertyDescriptors = new HashMap<String, PropertyDescriptor>(); // Empty and modifiable
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    		List<PropertyDescriptor> list = super.getSupportedPropertyDescriptors();
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
    public void onScheduled(final ProcessContext context) {
		getLogger().info("Correct version? 2017-10-13 22:41");
		
		super.onScheduled(context);

       	// Get static properties
		nofColumns = (int) context.getProperty(PREPARED_STATEMENT).getValue().chars().filter(c -> c == '?').count();
		
		// Get field names out of properties
		fieldNames = new String[nofColumns];
		for(int i = 1; i <= nofColumns; i++) {
			fieldNames[i-1] = context.getProperty("record-field-name." + i).getValue();
		}
		
    		getLogger().debug("Record-Field-Names: " + Arrays.deepToString(fieldNames));
    }

	@Override
	protected FlexProcessor createProcessor(ProcessContext context, ProcessSession session, Relationship success, Relationship failure,
			Relationship script, Connection scriptConnection, Connection loadConnection, PropertyValue beforeScript,
			PropertyValue preparedStatement, PropertyValue eltScript, PropertyValue afterScript,
			ComponentLog logger) throws SQLException {
		
       	// Initialize Record Reader and Writer Factory
		RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
		
		return new FlexProcessor(
				session, SUCCESS, FAILURE, SCRIPT,
				scriptConnection, loadConnection,
				beforeScript, preparedStatement, eltScript, afterScript,
				recordReaderFactory, fieldNames, getLogger());
	}
}
