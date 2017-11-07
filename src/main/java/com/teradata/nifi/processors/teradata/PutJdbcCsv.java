package com.teradata.nifi.processors.teradata;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

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
@SeeAlso({DBCPService.class})
public class PutJdbcCsv extends AbstractPutJdbc {
    private static final PropertyDescriptor CONNECTION_POOL_LOADING = CONNECTION_POOL_LOADING_BUILDER
            .required(true)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propertyDescriptors = super.getSupportedPropertyDescriptors();
        propertyDescriptors.add(1, CONNECTION_POOL_LOADING);
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context, CONNECTION_POOL_LOADING);
    }

    @Override
    protected int load(ProcessSession session, FlowFile flowFile, PreparedStatement statement, ComponentLog logger) throws IOException, SQLException {
        try(InputStream inputStream = session.read(flowFile)) {
            statement.setAsciiStream(1, inputStream, -1);
            return statement.executeUpdate();
        }
    }
}
