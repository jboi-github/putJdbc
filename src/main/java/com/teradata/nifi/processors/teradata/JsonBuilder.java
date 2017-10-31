package com.teradata.nifi.processors.teradata;

import org.apache.nifi.logging.ComponentLog;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Create Json-Document for script relationship.
 * The result contains:
 * - Timings like start, stop and elapsed milliseconds for the overall runtime.
 * - For each result set:
 *  - The result and update counts
 *  - Timings for the step like start, stop and elapsed milliseconds of taking result set.
 *  - Timing of the elapsed time between end for previous step and this step. This is equal to the time taken by the
 *      things happened in the meantime.
 */
public class JsonBuilder {
    private long firstStart;
    private long prevStop;
    private JSONArray stepsJson = new JSONArray();
    private ComponentLog logger;

    JsonBuilder(ComponentLog logger) {
        firstStart = System.currentTimeMillis();
        prevStop = firstStart;
        this.logger = logger;
    }

    /**
     * @param statement just executed and not closed statement that returns a mix of result sets and update counts.
     * @throws SQLException if anything goes wrong.
     */
    public void script(Statement statement, String script) throws SQLException {
        // Prepare for timings
        long start = System.currentTimeMillis();
        JSONObject stepJson = new JSONObject();

        // Execute script, read result set and update counts
        JSONArray resultSetsJson = new JSONArray();
        for(boolean isRows = statement.execute(script);; isRows = statement.getMoreResults()) {
            PutContentCsv.logWarnings(statement, logger);
            JSONObject resultSetJson = new JSONObject();

            if(isRows) {
                JSONArray rowsJson = new JSONArray();
                try (ResultSet resultSet = statement.getResultSet()) {
                    PutContentCsv.logWarnings(resultSet, logger);

                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                    while(resultSet.next()) {
                        JSONObject rowJson = new JSONObject();
                        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                            rowJson.put(resultSetMetaData.getColumnName(i), resultSet.getObject(i));
                        }
                        rowsJson.put(rowJson);
                    }

                    resultSetJson.put("rows", rowsJson);
                }
            } else {
                int updateCount = statement.getUpdateCount();
                if(updateCount == -1) {break;}

                resultSetJson.put("updateCount", updateCount);
            }
            resultSetsJson.put(resultSetJson);
        }

        // Add timings
        long stop = System.currentTimeMillis();
        stepJson.put("meantime", (start - prevStop));
        stepJson.put("start", start);
        stepJson.put("stop", stop);
        stepJson.put("elapsed", (stop - start));
        stepJson.put("resultSets", resultSetsJson);
        stepsJson.put(stepJson);

        // Prepare for next step
        prevStop = stop;
    }

    JSONObject close(AdditionalAttributes attributes, long size) {
        long stop = System.currentTimeMillis();

        JSONObject json = new JSONObject();
        json.put("start", firstStart);
        json.put("stop", stop);
        json.put("elapsed", stop - firstStart);
        json.put("size", size);
        json.put("attributes", attributes.getAsJson());
        json.put("steps", stepsJson);
        return json;
    }
}
