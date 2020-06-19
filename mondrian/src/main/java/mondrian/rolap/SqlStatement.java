/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2016 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.olap.Util.Functor1;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.server.monitor.SqlStatementEndEvent;
import mondrian.server.monitor.SqlStatementEvent;
import mondrian.server.monitor.SqlStatementEvent.Purpose;
import mondrian.server.monitor.SqlStatementExecuteEvent;
import mondrian.server.monitor.SqlStatementStartEvent;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;
import mondrian.util.Counters;
import mondrian.util.DelegatingInvocationHandler;

/**
 * SqlStatement contains a SQL statement and associated resources throughout
 * its lifetime.
 *
 * <p>The goal of SqlStatement is to make tracing, error-handling and
 * resource-management easier. None of the methods throws a SQLException;
 * if an error occurs in one of the methods, the method wraps the exception
 * in a {@link RuntimeException} describing the high-level operation, logs
 * that the operation failed, and throws that RuntimeException.
 *
 * <p>If methods succeed, the method generates lifecycle logging such as
 * the elapsed time and number of rows fetched.
 *
 * <p>There are a few obligations on the caller. The caller must:<ul>
 * <li>call the {@link #handle(Throwable)} method if one of the contained
 *     objects (say the {@link java.sql.ResultSet}) gives an error;
 * <li>call the {@link #close()} method if all operations complete
 *     successfully.
 * <li>increment the {@link #rowCount} field each time a row is fetched.
 * </ul>
 *
 * <p>The {@link #close()} method is idempotent. You are welcome to call it
 * more than once.
 *
 * <p>SqlStatement is not thread-safe.
 *
 * @author jhyde
 * @since 2.3
 */
public class SqlStatement {
    private static final String TIMING_NAME = "SqlStatement-";

    // used for SQL logging, allows for a SQL Statement UID
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private static final Semaphore querySemaphore = new Semaphore(MondrianProperties.instance().QueryLimit.get(), true);

    private final javax.sql.DataSource dataSource;
    private java.sql.Connection jdbcConnection;
    private java.sql.ResultSet resultSet;
    private final String sql;
    private final List<Type> types;
    private final int maxRows;
    private final int firstRowOrdinal;
    private final Locus locus;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private boolean haveSemaphore;
    public int rowCount;
    private long startTimeNanos;
    private long startTimeMillis;
    private final java.util.List<Accessor> accessors = new java.util.ArrayList<Accessor>();
    private State state = State.FRESH;
    private final long id;
    private final Functor1<Void, java.sql.Statement> callback;

    /**
     * Creates a SqlStatement.
     *
     * @param dataSource Data source
     * @param sql SQL
     * @param types Suggested types of columns, or null;
     *     if present, must have one element for each SQL column;
     *     each not-null entry overrides deduced JDBC type of the column
     * @param maxRows Maximum rows; <= 0 means no maximum
     * @param firstRowOrdinal Ordinal of first row to skip to; <= 0 do not skip
     * @param locus Execution context of this statement
     * @param resultSetType Result set type
     * @param resultSetConcurrency Result set concurrency
     */
    public SqlStatement(javax.sql.DataSource dataSource, String sql, java.util.List<mondrian.rolap.SqlStatement.Type> types, int maxRows,
        int firstRowOrdinal, Locus locus, int resultSetType, int resultSetConcurrency, Util.Functor1<Void, java.sql.Statement> callback) {
        this.callback = callback;
        this.id = ID_GENERATOR.getAndIncrement();
        this.dataSource = dataSource;
        this.sql = sql;
        this.types = types;
        this.maxRows = maxRows;
        this.firstRowOrdinal = firstRowOrdinal;
        this.locus = locus;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }

    /**
     * Executes the current statement, and handles any SQLException.
     */
    public void execute() {
        assert this.state == State.FRESH : "cannot re-execute";
        this.state = State.ACTIVE;
        Counters.SQL_STATEMENT_EXECUTE_COUNT.incrementAndGet();
        Counters.SQL_STATEMENT_EXECUTING_IDS.add(this.id);
        String status = "failed";
        java.sql.Statement statement = null;
        try {
            // Check execution state
            this.locus.execution.checkCancelOrTimeout();

            this.jdbcConnection = this.dataSource.getConnection();
            querySemaphore.acquire();
            this.haveSemaphore = true;
            // Trace start of execution.
            if (RolapUtil.SQL_LOGGER.isDebugEnabled()) {
                final StringBuilder sqllog = new StringBuilder();
                sqllog.append(this.id).append(": ").append(this.locus.component).append(": executing sql [");
                if (this.sql.indexOf('\n') >= 0) {
                    // SQL appears to be formatted as multiple lines. Make it
                    // start on its own line.
                    sqllog.append("\n");
                }
                sqllog.append(this.sql);
                sqllog.append(']');
                RolapUtil.SQL_LOGGER.debug(sqllog.toString());
            }

            // Execute hook.
            final RolapUtil.ExecuteQueryHook hook = RolapUtil.getHook();
            if (hook != null) {
                hook.onExecuteQuery(this.sql);
            }

            // Check execution state
            this.locus.execution.checkCancelOrTimeout();

            this.startTimeNanos = System.nanoTime();
            this.startTimeMillis = System.currentTimeMillis();

            if ((this.resultSetType < 0) || (this.resultSetConcurrency < 0)) {
                statement = this.jdbcConnection.createStatement();
            } else {
                statement = this.jdbcConnection.createStatement(this.resultSetType, this.resultSetConcurrency);
            }
            if (this.maxRows > 0) {
                statement.setMaxRows(this.maxRows);
            }

            // First make sure to register with the execution instance.
            if (this.getPurpose() != Purpose.CELL_SEGMENT) {
                this.locus.execution.registerStatement(this.locus, statement);
            } else {
                if (this.callback != null) {
                    this.callback.apply(statement);
                }
            }

            this.locus
                .getServer()
                    .getMonitor()
                    .sendEvent(new SqlStatementStartEvent(this.startTimeMillis, this.id, this.locus, this.sql, this.getPurpose(),
                        this.getCellRequestCount()));

            this.resultSet = statement.executeQuery(this.sql);

            // skip to first row specified in request
            this.state = State.ACTIVE;
            if (this.firstRowOrdinal > 0) {
                if (this.resultSetType == java.sql.ResultSet.TYPE_FORWARD_ONLY) {
                    for (int i = 0; i < this.firstRowOrdinal; ++i) {
                        if (!this.resultSet.next()) {
                            this.state = State.DONE;
                            break;
                        }
                    }
                } else {
                    if (!this.resultSet.absolute(this.firstRowOrdinal)) {
                        this.state = State.DONE;
                    }
                }
            }

            final long timeMillis = System.currentTimeMillis();
            final long timeNanos = System.nanoTime();
            final long executeNanos = timeNanos - this.startTimeNanos;
            final long executeMillis = executeNanos / 1000000;
            Util.addDatabaseTime(executeMillis);
            status = ", exec " + executeMillis + " ms";

            this.locus
                .getServer()
                    .getMonitor()
                    .sendEvent(new SqlStatementExecuteEvent(timeMillis, this.id, this.locus, this.sql, this.getPurpose(), executeNanos));

            // Compute accessors. They ensure that we use the most efficient
            // method (e.g. getInt, getDouble, getObject) for the type of the
            // column. Even if you are going to box the result into an object,
            // it is better to use getInt than getObject; the latter might
            // return something daft like a BigDecimal (does, on the Oracle JDBC
            // driver).
            this.accessors.clear();
            for (final Type type : this.guessTypes()) {
                this.accessors.add(this.createAccessor(this.accessors.size(), type));
            }
        } catch (final Throwable e) {
            status = ", failed (" + e + ")";

            // This statement was leaked to us. It is our responsibility
            // to dispose of it.
            Util.close(null, statement, null);

            // Now handle this exception.
            throw this.handle(e);
        } finally {
            RolapUtil.SQL_LOGGER.debug(this.id + ": " + status);

            if (RolapUtil.LOGGER.isDebugEnabled()) {
                RolapUtil.LOGGER.debug(this.locus.component + ": executing sql [" + this.sql + "]" + status);
            }
        }
    }

    /**
     * Closes all resources (statement, result set) held by this
     * SqlStatement.
     *
     * <p>If any of them fails, wraps them in a
     * {@link RuntimeException} describing the high-level operation which
     * this statement was performing. No further error-handling is required
     * to produce a descriptive stack trace, unless you want to absorb the
     * error.</p>
     *
     * <p>This method is idempotent.</p>
     */
    public void close() {
        if (this.state == State.CLOSED) {
            return;
        }
        this.state = State.CLOSED;

        if (this.haveSemaphore) {
            this.haveSemaphore = false;
            querySemaphore.release();
        }

        // According to the JDBC spec, closing a statement automatically closes
        // its result sets, and closing a connection automatically closes its
        // statements. But let's be conservative and close everything
        // explicitly.
        final java.sql.SQLException ex = Util.close(this.resultSet, null, this.jdbcConnection);
        this.resultSet = null;
        this.jdbcConnection = null;

        if (ex != null) {
            throw Util.newError(ex, this.locus.message + "; sql=[" + this.sql + "]");
        }

        final long endTime = System.currentTimeMillis();
        long totalMs;
        if (this.startTimeMillis == 0) {
            // execution didn't start at all
            totalMs = 0;
        } else {
            totalMs = endTime - this.startTimeMillis;
        }
        String status = this.formatTimingStatus(totalMs, this.rowCount);

        this.locus.execution.getQueryTiming().markFull(TIMING_NAME + this.locus.component, totalMs);

        RolapUtil.SQL_LOGGER.debug(this.id + ": " + status);

        Counters.SQL_STATEMENT_CLOSE_COUNT.incrementAndGet();
        final boolean remove = Counters.SQL_STATEMENT_EXECUTING_IDS.remove(this.id);
        status += ", ex=" + Counters.SQL_STATEMENT_EXECUTE_COUNT
            .get() + ", close=" + Counters.SQL_STATEMENT_CLOSE_COUNT.get() + ", open=" + Counters.SQL_STATEMENT_EXECUTING_IDS;

        if (RolapUtil.LOGGER.isDebugEnabled()) {
            RolapUtil.LOGGER.debug(this.locus.component + ": done executing sql [" + this.sql + "]" + status);
        }

        if (!remove) {
            throw new AssertionError("SqlStatement closed that was never executed: " + this.id);
        }

        this.locus
            .getServer()
                .getMonitor()
                .sendEvent(new SqlStatementEndEvent(endTime, this.id, this.locus, this.sql, this.getPurpose(), this.rowCount, false, null));
    }

    String formatTimingStatus(long totalMs, int rowCount) {
        return ", exec+fetch " + totalMs + " ms, " + rowCount + " rows";
    }

    public java.sql.ResultSet getResultSet() {
        return this.resultSet;
    }

    /**
     * Handles an exception thrown from the ResultSet, implicitly calls
     * {@link #close}, and returns an exception which includes the full
     * stack, including a description of the high-level operation.
     *
     * @param e Exception
     * @return Runtime exception
     */
    public RuntimeException handle(Throwable e) {
        final RuntimeException runtimeException = Util.newError(e, this.locus.message + "; sql=[" + this.sql + "]");
        try {
            this.close();
        } catch (final Throwable t) {
            // ignore
        }
        return runtimeException;
    }

    /**
     * Erzeugt ein DTO für den Zugriff
     * @param column
     * @param type
     * @return
     */
    private Accessor createAccessor(int column, mondrian.rolap.SqlStatement.Type type) {
        final int columnPlusOne = column + 1;
        switch (type) {
            case OBJECT:
                return new Accessor() {
                    @Override
                    public Object get()
                        throws java.sql.SQLException {
                        return SqlStatement.this.resultSet.getObject(columnPlusOne);
                    }
                };
            case STRING:
                return new Accessor() {
                    @Override
                    public Object get()
                        throws java.sql.SQLException {
                        return SqlStatement.this.resultSet.getString(columnPlusOne);
                    }
                };
            case INT:
                return new Accessor() {
                    @Override
                    public Object get()
                        throws java.sql.SQLException {
                        final int val = SqlStatement.this.resultSet.getInt(columnPlusOne);
                        if ((val == 0) && SqlStatement.this.resultSet.wasNull()) {
                            return null;
                        }
                        return val;
                    }
                };
            case LONG:
                return new Accessor() {
                    @Override
                    public Object get()
                        throws java.sql.SQLException {
                        final long val = SqlStatement.this.resultSet.getLong(columnPlusOne);
                        if ((val == 0) && SqlStatement.this.resultSet.wasNull()) {
                            return null;
                        }
                        return val;
                    }
                };
            case DOUBLE:
                return new Accessor() {
                    @Override
                    public Object get()
                        throws java.sql.SQLException {
                        final double val = SqlStatement.this.resultSet.getDouble(columnPlusOne);
                        if ((val == 0) && SqlStatement.this.resultSet.wasNull()) {
                            return null;
                        }
                        return val;
                    }
                };

            case DATE:
                return new Accessor() {
                    @Override
                    public java.lang.Object get()
                        throws java.sql.SQLException {
                        final java.sql.Date val = SqlStatement.this.resultSet.getDate(columnPlusOne);
                        if ((val == null) && SqlStatement.this.resultSet.wasNull()) {
                            return null;
                        }
                        return val;
                    }
                };
            case TIMESTAMP:
                return new Accessor() {
                    @Override
                    public java.lang.Object get()
                        throws java.sql.SQLException {
                        final java.sql.Timestamp val = SqlStatement.this.resultSet.getTimestamp(columnPlusOne);
                        if ((val == null) && SqlStatement.this.resultSet.wasNull()) {
                            return null;
                        }
                        return val;
                    }
                };
            case TIME:
                return new Accessor() {
                    @Override
                    public java.lang.Object get()
                        throws java.sql.SQLException {
                        final java.sql.Time val = SqlStatement.this.resultSet.getTime(columnPlusOne);
                        if ((val == null) && SqlStatement.this.resultSet.wasNull()) {
                            return null;
                        }
                        return val;
                    }
                };
            default:
                throw Util.unexpected(type);
        }
    }

    public java.util.List<mondrian.rolap.SqlStatement.Type> guessTypes()
        throws java.sql.SQLException {
        final java.sql.ResultSetMetaData metaData = this.resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        assert (this.types == null) || (this.types.size() == columnCount);
        final java.util.List<mondrian.rolap.SqlStatement.Type> types = new java.util.ArrayList<mondrian.rolap.SqlStatement.Type>();

        for (int i = 0; i < columnCount; i++) {
            final mondrian.rolap.SqlStatement.Type suggestedType = this.types == null ? null : this.types.get(i);
            // There might not be a schema constructed yet,
            // so watch out here for NPEs.
            final RolapSchema schema = this.locus.execution.getMondrianStatement().getMondrianConnection().getSchema();

            final Dialect dialect = this.getDialect(schema);

            if (suggestedType != null) {
                types.add(suggestedType);
            } else if (dialect != null) {
                types.add(dialect.getType(metaData, i));
            } else {
                types.add(mondrian.rolap.SqlStatement.Type.OBJECT);
            }
        }
        return types;
    }

    /**
     * Retrieves dialect from schema or attempts to create it
     * in case it is null
     *
     * @param schema rolap schema
     * @return database dialect
     */
    protected Dialect getDialect(RolapSchema schema) {
        Dialect dialect = null;
        if ((schema != null) && (schema.getDialect() != null)) {
            dialect = schema.getDialect();
        } else {
            dialect = this.createDialect();
        }
        return dialect;
    }

    /**
     * For tests
     */
    protected Dialect createDialect() {
        return DialectManager.createDialect(this.dataSource, this.jdbcConnection);
    }

    public List<Accessor> getAccessors()
        throws java.sql.SQLException {
        return this.accessors;
    }

    /**
     * Returns the result set in a proxy which automatically closes this
     * SqlStatement (and hence also the statement and result set) when the
     * result set is closed.
     *
     * <p>This helps to prevent connection leaks. The caller still has to
     * remember to call ResultSet.close(), of course.
     *
     * @return Wrapped result set
     */
    public java.sql.ResultSet getWrappedResultSet() {
        return (java.sql.ResultSet) Proxy
            .newProxyInstance(null, new Class<?>[] { java.sql.ResultSet.class }, new MyDelegatingInvocationHandler(this));
    }

    private SqlStatementEvent.Purpose getPurpose() {
        if (this.locus instanceof StatementLocus) {
            return ((StatementLocus) this.locus).purpose;
        } else {
            return SqlStatementEvent.Purpose.OTHER;
        }
    }

    private int getCellRequestCount() {
        if (this.locus instanceof StatementLocus) {
            return ((StatementLocus) this.locus).cellRequestCount;
        } else {
            return 0;
        }
    }

    /**
     * The approximate JDBC type of a column.
     *
     * <p>This type affects which {@link java.sql.ResultSet} method we use to get values
     * of this column: the default is {@link java.sql.ResultSet#getObject(int)},
     * but we'd prefer to use native values {@code getInt} and {@code getDouble}
     * if possible.
     */
    public enum Type {
        OBJECT, DOUBLE, INT, LONG, STRING, DATE, TIMESTAMP, TIME;

        public java.lang.Object get(java.sql.ResultSet resultSet, int column)
            throws java.sql.SQLException {
            switch (this) {
                case OBJECT:
                    return resultSet.getObject(column + 1);
                case STRING:
                    return resultSet.getString(column + 1);
                case INT:
                    return resultSet.getInt(column + 1);
                case LONG:
                    return resultSet.getLong(column + 1);
                case DOUBLE:
                    return resultSet.getDouble(column + 1);
                case DATE:
                    return resultSet.getDate(column + 1);
                case TIMESTAMP:
                    return resultSet.getTimestamp(column + 1);
                case TIME:
                    return resultSet.getTime(column + 1);

                default:
                    throw Util.unexpected(this);
            }
        }
    }

    public interface Accessor {
        java.lang.Object get()
            throws java.sql.SQLException;
    }

    /**
     * Reflectively implements the {@link java.sql.ResultSet} interface by routing method
     * calls to the result set inside a {@link mondrian.rolap.SqlStatement}.
     * When the result set is closed, so is the SqlStatement, and hence the
     * JDBC connection and statement also.
     */
    // must be public for reflection to work
    public static class MyDelegatingInvocationHandler extends DelegatingInvocationHandler {
        private final SqlStatement sqlStatement;

        /**
         * Creates a MyDelegatingInvocationHandler.
         *
         * @param sqlStatement SQL statement
         */
        MyDelegatingInvocationHandler(SqlStatement sqlStatement) {
            this.sqlStatement = sqlStatement;
        }

        @Override
        protected Object getTarget()
            throws InvocationTargetException {
            final java.sql.ResultSet resultSet = this.sqlStatement.getResultSet();
            if (resultSet == null) {
                throw new InvocationTargetException(new java.sql.SQLException("Invalid operation. Statement is closed."));
            }
            return resultSet;
        }

        /**
         * Helper method to implement {@link java.sql.ResultSet#close()}.
         *
         * @throws java.sql.SQLException on error
         */
        public void close()
            throws java.sql.SQLException {
            this.sqlStatement.close();
        }
    }

    private enum State {
        FRESH, ACTIVE, DONE, CLOSED
    }

    public static class StatementLocus extends Locus {
        private final SqlStatementEvent.Purpose purpose;
        private final int cellRequestCount;

        public StatementLocus(Execution execution, String component, String message, SqlStatementEvent.Purpose purpose,
            int cellRequestCount) {
            super(execution, component, message);
            this.purpose = purpose;
            this.cellRequestCount = cellRequestCount;
        }
    }
}

// End SqlStatement.java
