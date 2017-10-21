package com.teradata.nifi.processors.teradata;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

@Tags({"Teradata", "SQL", "load", "database", "JDBC", "bulk", "fastload", "ELT", "relational", "RDBMS", "CSV"})
@CapabilityDescription(
		"Load bulk amount of data into Teradata using Fastload-CSV capabilities. "
		+ "Define an insert statement for a table to land the data and have this processor start scripts for mini-batched ELT. "
		+ "The Loader uses a landing tables to load into. It first runs a script before it loads into the table. When the script "
		+ "has finished load of table starts for one FLowFile containing CSV data. After that, this processor runs the ELT script "
		+ "and commits its work. When th Queue gets empty or the User stops this processor, a final commit-sequence (database-commit, "
		+ "ELT Script, Nifi-commit) runs and the after script is started. Any exception that happens will roll back and yield the "
		+ "processor if it was not a transient error. The processor works only, if the before script ensures an empty and exclusive "
		+ "table to load the data. The data must be CSV data in one FlowFile per load. Ensure that, the Connection-URL states the "
		+ "correct type=FASTLOADCSV, Field-separator, character set etc. ALso ensure, that this parameter match to the CSV data in the FLowFile. "
		+ "A common usage pattern is, that a ConvertRecord parses incoming data into CSV-Files and MergeContent put together a larger Flowfile "
		+ "for efficient Fastload-Loading. "
		+ "Thus, together with the commit and roll back for a session in Nifi ensures, that Flow Files "
		+ "are always loaded once and only once into the database.")
@WritesAttributes({
	@WritesAttribute(
			attribute = "TABLE_ID",
			description = "A 10 characters long random value ontaining only digits (0-9) and "
				+ "lowercase letters (a-z). This attribute can be used to construct a table "
				+ "name which will be unique in the far most cases."),
	@WritesAttribute(
			attribute = "COMMIT_EPOCH",
			description = "An integer counting how often this task locally ran thru the commit sequence. "
					+ "The number can be loaded into the load table and helpd to identify in the ELT which "
					+ "rows where loaded in latest batch.")
	})
//@TriggerSerially
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso({DBCPService.class})

public class PutJdbcCsv extends AbstractPutJdbc {
	/**
	 * @param context that onScheduled is called from
	 */
	@OnScheduled
    public void onScheduled(final ProcessContext context) {
		getLogger().info("Correct version? 2017-10-20 15:41 CSV");
		super.onScheduled(context);
    }
    
	/* (non-Javadoc)
	 * @see com.teradata.nifi.processors.teradata.AbstractPutJdbcHighSpeed#createProcessor(org.apache.nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession, org.apache.nifi.processor.Relationship, org.apache.nifi.processor.Relationship, org.apache.nifi.processor.Relationship, java.sql.Connection, java.sql.Connection, org.apache.nifi.components.PropertyValue, org.apache.nifi.components.PropertyValue, org.apache.nifi.components.PropertyValue, org.apache.nifi.components.PropertyValue, org.apache.nifi.logging.ComponentLog)
	 */
	@Override
	protected CsvProcessor createProcessor(
			ProcessContext context, ProcessSession session, Relationship success, Relationship failure,
			Relationship script, Connection scriptConnection, Connection loadConnection, PropertyValue beforeScript,
			PropertyValue preparedStatement, PropertyValue eltScript, PropertyValue afterScript,
			ComponentLog logger) throws SQLException {
		
		return new CsvProcessor(
				session, SUCCESS, FAILURE, SCRIPT,
				scriptConnection, loadConnection,
				beforeScript, preparedStatement, eltScript, afterScript,
				getLogger());
	}
}
