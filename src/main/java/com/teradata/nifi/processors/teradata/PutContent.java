package com.teradata.nifi.processors.teradata;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.logging.ComponentLog;
import org.json.JSONObject;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;

/**
 * Keeps a connection to the database and processes FlowFile by FlowFile using
 * Teradata's Fastload-CSV capability.
 */
public class PutContent implements AutoCloseable {
    @FunctionalInterface
    public interface FlowFileProcessor {
        int process(PreparedStatement statement) throws SQLException, IOException;
    }
    private Connection primary;
    private String beforeDisconnecting;
    private AdditionalAttributes attributes;
    private ComponentLog logger;

    /**
     * @param connectionPoolPrimary to get primary connection.
     * @param afterConnected script containing SQL statements to be executed after connected on primary connection.
     * @param beforeDisconnecting script containing SQL statements to be executed before disconnecting on primary connection.
     * @param attributes for TABLE_ID and COMMIT_EPOCH
     * @param logger to log messages, warnings.
     * @throws SQLException When database error occurred.
     */
    PutContent(
            DBCPService connectionPoolPrimary,
            String afterConnected, String beforeDisconnecting,
            AdditionalAttributes attributes, ComponentLog logger) throws SQLException
    {
        this.beforeDisconnecting = beforeDisconnecting;
        this.logger = logger;
        this.attributes = attributes;

        // Connecting sequence
        primary = connectionPoolPrimary.getConnection();
        logWarnings(primary, logger);
        executeScriptIgnoreResult(afterConnected, "afterConnected");
    }

    /**
     * @param size of input stream.
     * @param connectionPoolLoading to get connection for loading. If null then primary is used for loading.
     * @param beforeLoading script containing SQL statements to be executed before loading on primary connection.
     * @param insertStatement for loading rows on loading connection.
     * @param afterLoaded script containing SQL statements to be executed after loaded on primary connection.
     * @return JSON document with timings and results of scripts.
     * @throws SQLException When database error occurred.
     */
    JSONObject process(
            long size, DBCPService connectionPoolLoading,
            String beforeLoading, String insertStatement, String afterLoaded,
            FlowFileProcessor flowFileProcessor) throws SQLException, IOException {
        JsonBuilder json = new JsonBuilder(logger);
        executeScript(json, beforeLoading, "beforeLoading");

        long start = System.currentTimeMillis();
        int updateCount;
        Connection loading = (connectionPoolLoading != null)? connectionPoolLoading.getConnection():primary;
        if(connectionPoolLoading != null) loading.setAutoCommit(false);
        try {
            // Connect
            logWarnings(loading, logger);
            loading.setAutoCommit(false);
            logWarnings(loading, logger);

            try (PreparedStatement preparedStatement = loading.prepareStatement(insertStatement)) {
                logWarnings(preparedStatement, logger);

                updateCount = flowFileProcessor.process(preparedStatement);
                logWarnings(preparedStatement, logger);
            }

            // Disconnect
            if(!loading.getAutoCommit()) loading.commit();
            logWarnings(loading, logger);
        } catch (SQLException | IOException e) {
            if(connectionPoolLoading != null && !loading.getAutoCommit()) loading.rollback();
            throw e;
        } finally {
            if(connectionPoolLoading != null) {
                if(!loading.getAutoCommit()) loading.commit();
                loading.close();
            }
        }
        long stop = System.currentTimeMillis();
        JSONObject loadJson = new JSONObject();
        loadJson.put("start", start);
        loadJson.put("stop", stop);
        loadJson.put("elapsed", (stop - start));
        loadJson.put("updateCount", updateCount);

        executeScript(json, afterLoaded, "afterLoaded");
        JSONObject resultJson = json.close(attributes, size);
        resultJson.put("load", loadJson);
        return resultJson;
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     * <p>
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     * <p>
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     * <p>
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     * <p>
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        executeScriptIgnoreResult(beforeDisconnecting, "beforeDisconnecting");
        primary.close();
    }

    /**
     * @param script containing SQL statements to be executed
     * @throws SQLException When database error occurred.
     */
    private void executeScript(JsonBuilder json, String script, String scriptName) throws SQLException {
        if(script != null && !script.trim().isEmpty()) {
            String evaluated = attributes.evaluate(script);
            logger.debug("{} runs as \"{}\"", new Object[] {scriptName, evaluated});
            json.script(primary.createStatement(), evaluated);
        } else {
            logger.debug("{} is empty", new Object[] {scriptName});
        }
    }

    /**
     * @param script containing SQL statements to be executed
     * @throws SQLException When database error occurred.
     */
    private void executeScriptIgnoreResult(String script, String scriptName) throws SQLException {
        JsonBuilder json = new JsonBuilder(logger);
        executeScript(json, script, scriptName);
        json.close(attributes, 0L);
    }

    /**
     * @param connection to print warnings for.
     * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
     */
    static private void logWarnings(Connection connection, ComponentLog logger) throws SQLException {
        logWarnings(connection.getWarnings(), logger);
        connection.clearWarnings();
    }

    /**
     * @param statement to print warnings for.
     * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
     */
    static void logWarnings(Statement statement, ComponentLog logger) throws SQLException {
        logWarnings(statement.getWarnings(), logger);
        statement.clearWarnings();
    }

    /**
     * @param resultSet to print warnings for.
     * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
     */
    static void logWarnings(ResultSet resultSet, ComponentLog logger) throws SQLException {
        PutContent.logWarnings(resultSet.getWarnings(), logger);
        resultSet.clearWarnings();
    }

    /**
     * @param warning as starting point to print chain of warnings.
     * @throws SQLException whenever an error occurred while working with DB. Warnings are logged but not thrown.
     */
    private static void logWarnings(SQLWarning warning, ComponentLog logger) throws SQLException {
        for(; warning != null; warning = warning.getNextWarning()) {
            logger.info(
                    "{} (SQL State = {}), (error code = {})",
                    new Object[] {warning.getMessage(), warning.getSQLState(), warning.getErrorCode()});
        }
    }
}
