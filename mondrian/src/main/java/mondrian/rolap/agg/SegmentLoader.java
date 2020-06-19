/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2018 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import mondrian.olap.MondrianException;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.SqlStatement;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.agg.SegmentCacheManager.AbortException;
import mondrian.rolap.cache.SegmentCacheIndex;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.server.monitor.SqlStatementEvent;
import mondrian.spi.SegmentBody;
import mondrian.spi.SegmentColumn;
import mondrian.spi.SegmentHeader;
import mondrian.util.CancellationChecker;
import mondrian.util.Pair;

/**
 * <p>The <code>SegmentLoader</code> queries database and loads the data into
 * the given set of segments.</p>
 *
 * <p>It reads a segment of <code>measure</code>, where <code>columns</code>
 * are constrained to <code>values</code>.  Each entry in <code>values</code>
 * can be null, meaning don't constrain, or can have several values. For
 * example, <code>getSegment({Unit_sales}, {Region, State, Year}, {"West"},
 * {"CA", "OR", "WA"}, null})</code> returns sales in states CA, OR and WA
 * in the Western region, for all years.</p>
 *
 * <p>It will also look at the {@link MondrianProperties#SegmentCache} property
 * and make usage of the SegmentCache provided as an SPI.
 *
 * @author Thiyagu, LBoudreau
 * @since 24 May 2007
 */
public class SegmentLoader {

    private static final Logger LOGGER = Logger.getLogger(SegmentLoader.class);

    private final SegmentCacheManager cacheMgr;

    /**
     * Creates a SegmentLoader.
     *
     * @param cacheMgr Cache manager
     */
    public SegmentLoader(SegmentCacheManager cacheMgr) {
        this.cacheMgr = cacheMgr;
    }

    /**
     * Loads data for all the segments of the GroupingSets. If the grouping sets
     * list contains more than one Grouping Set then data is loaded using the
     * GROUP BY GROUPING SETS sql. Else if only one grouping set is passed in
     * the list data is loaded without using GROUP BY GROUPING SETS sql. If the
     * database does not support grouping sets
     * {@link mondrian.spi.Dialect#supportsGroupingSets()} then
     * grouping sets list should always have only one element in it.
     *
     * <p>For example, if list has 2 grouping sets with columns A, B, C and B, C
     * respectively, then the SQL will be
     * "GROUP BY GROUPING SETS ((A, B, C), (B, C))".
     *
     * <p>Else if the list has only one grouping set then sql would be without
     * grouping sets.
     *
     * <p>The <code>groupingSets</code> list should be topological order, with
     * more detailed higher-level grouping sets occurring first. In other words,
     * the first element of the list should always be the detailed grouping
     * set (default grouping set), followed by grouping sets which can be
     * rolled-up on this detailed grouping set.
     * In the example (A, B, C) is the detailed grouping set and (B, C) is
     * rolled-up using the detailed.
     *
     * <p>Grouping sets are removed from the {@code groupingSets} list as they
     * are loaded.</p>
     *
     * @param cellRequestCount Number of missed cells that led to this request
     * @param groupingSets   List of grouping sets whose segments are loaded
     * @param compoundPredicateList Compound predicates
     * @param segmentFutures List of futures wherein each statement will place
     *                       a list of the segments it has loaded, when it
     *                       completes
     */
    public void load(int cellRequestCount,
        List<GroupingSet> groupingSets,
        List<StarPredicate> compoundPredicateList,
        List<Future<Map<Segment, SegmentWithData>>> segmentFutures) {
        if (!MondrianProperties.instance().DisableCaching.get()) {
            for (final GroupingSet groupingSet : groupingSets) {
                for (final Segment segment : groupingSet.getSegments()) {
                    final SegmentCacheIndex index = this.cacheMgr.getIndexRegistry().getIndex(segment.star);
                    index.add(segment.getHeader(), new SegmentBuilder.StarSegmentConverter(segment.measure, compoundPredicateList), true);
                    // Make sure that we are registered as a client of
                    // the segment by invoking getFuture.
                    Util.discard(index.getFuture(Locus.peek().execution, segment.getHeader()));
                }
            }
        }
        try {
            segmentFutures
                .add(this.cacheMgr.sqlExecutor
                    .submit(new SegmentLoadCommand(Locus.peek(), this, cellRequestCount, groupingSets, compoundPredicateList)));
        } catch (final Exception e) {
            throw new MondrianException(e);
        }
    }

    private static class SegmentLoadCommand implements Callable<Map<Segment, SegmentWithData>> {
        private final Locus locus;
        private final SegmentLoader segmentLoader;
        private final int cellRequestCount;
        private final List<GroupingSet> groupingSets;
        private final List<StarPredicate> compoundPredicateList;

        public SegmentLoadCommand(Locus locus, SegmentLoader segmentLoader, int cellRequestCount, List<GroupingSet> groupingSets,
            List<StarPredicate> compoundPredicateList) {
            this.locus = locus;
            this.segmentLoader = segmentLoader;
            this.cellRequestCount = cellRequestCount;
            this.groupingSets = groupingSets;
            this.compoundPredicateList = compoundPredicateList;
        }

        @Override
        public Map<Segment, SegmentWithData> call()
            throws Exception {
            Locus.push(this.locus);
            try {
                return this.segmentLoader.loadImpl(this.cellRequestCount, this.groupingSets, this.compoundPredicateList);
            } finally {
                Locus.pop(this.locus);
            }
        }
    }

    private Map<Segment, SegmentWithData> loadImpl(int cellRequestCount,
        List<GroupingSet> groupingSets,
        List<StarPredicate> compoundPredicateList) {
        SqlStatement stmt = null;
        final GroupingSetsList groupingSetsList = new GroupingSetsList(groupingSets);
        final RolapStar.Column[] defaultColumns = groupingSetsList.getDefaultColumns();

        final Map<Segment, SegmentWithData> segmentMap = new HashMap<Segment, SegmentWithData>();
        Throwable throwable = null;
        try {
            final int arity = defaultColumns.length;
            final SortedSet<Comparable>[] axisValueSets = this.getDistinctValueWorkspace(arity);

            stmt = this.createExecuteSql(cellRequestCount, groupingSetsList, compoundPredicateList);

            if (stmt == null) {
                // Nothing to do. We're done here.
                return segmentMap;
            }

            final boolean[] axisContainsNull = new boolean[arity];

            final RowList rows = this.processData(stmt, axisContainsNull, axisValueSets, groupingSetsList);

            final boolean sparse = this.setAxisDataAndDecideSparseUse(axisValueSets, axisContainsNull, groupingSetsList, rows);

            final Map<BitKey, GroupingSetsList.Cohort> groupingDataSetsMap = this
                .createDataSetsForGroupingSets(groupingSetsList, sparse, rows.getTypes().subList(arity, rows.getTypes().size()));

            this.loadDataToDataSets(groupingSetsList, rows, groupingDataSetsMap);

            this.setDataToSegments(groupingSetsList, groupingDataSetsMap, segmentMap);

            return segmentMap;
        } catch (final Throwable e) {
            throwable = e;
            if (stmt == null) {
                throw new MondrianException(e);
            }
            throw stmt.handle(e);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            this.setFailOnStillLoadingSegments(segmentMap, groupingSetsList, throwable);
        }
    }

    /**
     * Called when a segment has been loaded from SQL, to put into the segment
     * index and the external cache.
     *
     * @param header Segment header
     * @param body Segment body
     */
    private void cacheSegment(RolapStar star, SegmentHeader header, SegmentBody body) {
        // Write the segment into external cache.
        //
        // It would be a mistake to do this from the cacheMgr -- because the
        // calls may take time. The cacheMgr's actions must all be quick. We
        // are a worker, so we have plenty of time.
        //
        // Also note that we push the segments to external cache after we have
        // called cacheMgr.loadSucceeded. That call will allow the current
        // query to proceed.
        if (!MondrianProperties.instance().DisableCaching.get()) {
            this.cacheMgr.compositeCache.put(header, body);
            this.cacheMgr.loadSucceeded(star, header, body);
        }
    }

    private boolean setFailOnStillLoadingSegments(Map<Segment, SegmentWithData> segmentMap,
        GroupingSetsList groupingSetsList,
        Throwable throwable) {
        int n = 0;
        for (final GroupingSet groupingSet : groupingSetsList.getGroupingSets()) {
            for (final Segment segment : groupingSet.getSegments()) {
                if (!segmentMap.containsKey(segment)) {
                    if (throwable == null) {
                        throwable = new RuntimeException("Segment failed to load");
                    }
                    final SegmentHeader header = segment.getHeader();
                    this.cacheMgr.loadFailed(segment.star, header, throwable);
                    ++n;
                }
            }
        }
        return n > 0;
    }

    /**
     * Loads data to the datasets. If the grouping sets is used,
     * dataset is fetched from groupingDataSetMap using grouping bit keys of
     * the row data. If grouping sets is not used, data is loaded on to
     * nonGroupingDataSets.
     */
    private void loadDataToDataSets(GroupingSetsList groupingSetsList,
        RowList rows,
        Map<BitKey, GroupingSetsList.Cohort> groupingDataSetMap) {
        final int arity = groupingSetsList.getDefaultColumns().length;
        final SegmentAxis[] axes = groupingSetsList.getDefaultAxes();
        final int segmentLength = groupingSetsList.getDefaultSegments().size();

        final List<SqlStatement.Type> types = rows.getTypes();
        final boolean useGroupingSet = groupingSetsList.useGroupingSets();
        for (rows.first(); rows.next();) {
            final BitKey groupingBitKey;
            final GroupingSetsList.Cohort cohort;
            if (useGroupingSet) {
                groupingBitKey = (BitKey) rows.getObject(groupingSetsList.getGroupingBitKeyIndex());
                cohort = groupingDataSetMap.get(groupingBitKey);
            } else {
                groupingBitKey = null;
                cohort = groupingDataSetMap.get(BitKey.EMPTY);
            }
            final int[] pos = cohort.pos;
            for (int j = 0, k = 0; j < arity; j++) {
                final SqlStatement.Type type = types.get(j);
                switch (type) {
                    // TODO: different treatment for INT, LONG, DOUBLE
                    case OBJECT:
                    case STRING:
                    case DATE:
                    case TIMESTAMP:
                    case TIME:
                    case INT:
                    case LONG:
                    case DOUBLE:
                        Object o = rows.getObject(j);
                        if (useGroupingSet && ((o == null) || (o == RolapUtil.sqlNullValue))
                            && groupingBitKey.get(groupingSetsList.findGroupingFunctionIndex(j))) {
                            continue;
                        }
                        final SegmentAxis axis = axes[j];
                        if (o == null) {
                            o = RolapUtil.sqlNullValue;
                        }
                        // Note: We believe that all value types are Comparable.
                        // In JDK 1.4, Boolean did not implement Comparable, but
                        // that's too minor/long ago to worry about.
                        final int offset = axis.getOffset((Comparable) o);
                        pos[k++] = offset;
                        break;
                    default:
                        throw Util.unexpected(type);
                }
            }

            for (int j = 0; j < segmentLength; j++) {
                cohort.segmentDatasetList.get(j).populateFrom(pos, rows, arity + j);
            }
        }
    }

    private boolean setAxisDataAndDecideSparseUse(SortedSet<Comparable>[] axisValueSets,
        boolean[] axisContainsNull,
        GroupingSetsList groupingSetsList,
        RowList rows) {
        final SegmentAxis[] axes = groupingSetsList.getDefaultAxes();
        final RolapStar.Column[] allColumns = groupingSetsList.getDefaultColumns();
        // Figure out size of dense array, and allocate it, or use a sparse
        // array if appropriate.
        boolean sparse = false;
        int n = 1;
        for (int i = 0; i < axes.length; i++) {
            final SortedSet<Comparable> valueSet = axisValueSets[i];
            axes[i] = new SegmentAxis(groupingSetsList.getDefaultPredicates()[i], valueSet, axisContainsNull[i]);
            final int size = axes[i].getKeys().length;
            this.setAxisDataToGroupableList(groupingSetsList, valueSet, axisContainsNull[i], allColumns[i]);
            final int previous = n;
            n *= size;
            if ((n < previous) || (n < size)) {
                // Overflow has occurred.
                n = Integer.MAX_VALUE;
                sparse = true;
            }
        }
        return this.useSparse(sparse, n, rows);
    }

    boolean useSparse(boolean sparse, int n, RowList rows) {
        sparse = sparse || useSparse(n, rows.size());
        return sparse;
    }

    private void setDataToSegments(GroupingSetsList groupingSetsList,
        Map<BitKey, GroupingSetsList.Cohort> datasetsMap,
        Map<Segment, SegmentWithData> segmentSlotMap) {
        final List<GroupingSet> groupingSets = groupingSetsList.getGroupingSets();
        for (int i = 0; i < groupingSets.size(); i++) {
            final List<Segment> segments = groupingSets.get(i).getSegments();
            final GroupingSetsList.Cohort cohort = datasetsMap.get(groupingSetsList.getRollupColumnsBitKeyList().get(i));
            for (int j = 0; j < segments.size(); j++) {
                final Segment segment = segments.get(j);
                final SegmentDataset segmentDataset = cohort.segmentDatasetList.get(j);
                final SegmentWithData segmentWithData = new SegmentWithData(segment, segmentDataset, cohort.axes);

                segmentSlotMap.put(segment, segmentWithData);

                final SegmentHeader header = segmentWithData.getHeader();
                final SegmentBody body = segmentWithData
                    .getData()
                        .createSegmentBody(new AbstractList<Pair<SortedSet<Comparable>, Boolean>>() {
                            @Override
                            public Pair<SortedSet<Comparable>, Boolean> get(int index) {
                                return segmentWithData.axes[index].getValuesAndIndicator();
                            }

                            @Override
                            public int size() {
                                return segmentWithData.axes.length;
                            }
                        });

                // Send a message to the agg manager. It will place the segment
                // in the index.
                this.cacheSegment(segment.star, header, body);
            }
        }
    }

    private Map<BitKey, GroupingSetsList.Cohort> createDataSetsForGroupingSets(GroupingSetsList groupingSetsList,
        boolean sparse,
        List<SqlStatement.Type> types) {
        if (!groupingSetsList.useGroupingSets()) {
            final GroupingSetsList.Cohort datasets = this
                .createDataSets(sparse, groupingSetsList.getDefaultSegments(), groupingSetsList.getDefaultAxes(), types);
            return Collections.singletonMap(BitKey.EMPTY, datasets);
        }
        final Map<BitKey, GroupingSetsList.Cohort> datasetsMap = new HashMap<BitKey, GroupingSetsList.Cohort>();
        final List<GroupingSet> groupingSets = groupingSetsList.getGroupingSets();
        final List<BitKey> groupingColumnsBitKeyList = groupingSetsList.getRollupColumnsBitKeyList();
        for (int i = 0; i < groupingSets.size(); i++) {
            final GroupingSet groupingSet = groupingSets.get(i);
            final GroupingSetsList.Cohort cohort = this.createDataSets(sparse, groupingSet.getSegments(), groupingSet.getAxes(), types);
            datasetsMap.put(groupingColumnsBitKeyList.get(i), cohort);
        }
        return datasetsMap;
    }

    private int calculateMaxDataSize(SegmentAxis[] axes) {
        int n = 1;
        for (final SegmentAxis axis : axes) {
            n *= axis.getKeys().length;
        }
        return n;
    }

    private GroupingSetsList.Cohort createDataSets(boolean sparse,
        List<Segment> segments,
        SegmentAxis[] axes,
        List<SqlStatement.Type> types) {
        final List<SegmentDataset> datasets = new ArrayList<SegmentDataset>(segments.size());
        final int n;
        if (sparse) {
            n = 0;
        } else {
            n = this.calculateMaxDataSize(axes);
        }
        for (int i = 0; i < segments.size(); i++) {
            final Segment segment = segments.get(i);
            datasets.add(segment.createDataset(axes, sparse, types.get(i), n));
        }
        return new GroupingSetsList.Cohort(datasets, axes);
    }

    private void setAxisDataToGroupableList(GroupingSetsList groupingSetsList,
        SortedSet<Comparable> valueSet,
        boolean axisContainsNull,
        RolapStar.Column column) {
        for (final GroupingSet groupingSet : groupingSetsList.getRollupGroupingSets()) {
            final RolapStar.Column[] columns = groupingSet.getColumns();
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equals(column)) {
                    groupingSet.getAxes()[i] = new SegmentAxis(groupingSet.getPredicates()[i], valueSet, axisContainsNull);
                }
            }
        }
    }

    /**
     * Creates and executes a SQL statement to retrieve the set of cells
     * specified by a GroupingSetsList.
     *
     * <p>This method may be overridden in tests.
     *
     * @param cellRequestCount Number of missed cells that led to this request
     * @param groupingSetsList Grouping
     * @param compoundPredicateList Compound predicate list
     * @return An executed SQL statement, or null
     */
    SqlStatement createExecuteSql(int cellRequestCount,
        final GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList) {
        final RolapStar star = groupingSetsList.getStar();
        final Pair<String, List<SqlStatement.Type>> pair = AggregationManager.generateSql(groupingSetsList, compoundPredicateList);
        final Locus locus = new SqlStatement.StatementLocus(Locus.peek().execution, "Segment.load", "Error while loading segment",
            SqlStatementEvent.Purpose.CELL_SEGMENT, cellRequestCount);

        // When caching is enabled, we must register the SQL statement
        // in the index. We don't want to cancel SQL statements that are shared
        // across threads unless it is safe.
        final Util.Functor1<Void, Statement> callbackWithCaching = new Util.Functor1<Void, Statement>() {
            @Override
            public Void apply(final Statement stmt) {
                SegmentLoader.this.cacheMgr.execute(new SegmentCacheManager.Command<Void>() {
                    @Override
                    public Void call()
                        throws Exception {
                        boolean atLeastOneActive = false;
                        for (final Segment seg : groupingSetsList.getDefaultSegments()) {
                            final SegmentCacheIndex index = SegmentLoader.this.cacheMgr.getIndexRegistry().getIndex(seg.star);
                            // Make sure to check if the segment still
                            // exists in the index. It could have been
                            // removed by a cancellation request since
                            // then.
                            if (index.contains(seg.getHeader())) {
                                index.linkSqlStatement(seg.getHeader(), stmt);
                                atLeastOneActive = true;
                            }
                            if (!atLeastOneActive) {
                                // There are no segments to load.
                                // Throw this so that the segment thread
                                // knows to stop.
                                throw new AbortException();
                            }
                        }
                        return null;
                    }
                    @Override
                    public Locus getLocus() {
                        return locus;
                    }
                });
                return null;
            }
        };

        // When using no cache, we register the SQL statement directly
        // with the execution instance for cleanup.
        final Util.Functor1<Void, Statement> callbackNoCaching = new Util.Functor1<Void, Statement>() {
            @Override
            public Void apply(final Statement stmt) {
                locus.execution.registerStatement(locus, stmt);
                return null;
            }
        };

        try {
            return RolapUtil
                .executeQuery(star.getDataSource(),
                    pair.left,
                    pair.right,
                    0,
                    0,
                    locus,
                    -1,
                    -1,
                    // Only one of the two callbacks are required, depending if we
                    // cache the segments or not.
                    MondrianProperties.instance().DisableCaching.get() ? callbackNoCaching : callbackWithCaching);
        } catch (final Throwable t) {
            if (Util.getMatchingCause(t, AbortException.class) != null) {
                return null;
            } else {
                throw new MondrianException("Failed to load segment form SQL", t);
            }
        }
    }

    RowList processData(SqlStatement stmt,
        final boolean[] axisContainsNull,
        final SortedSet<Comparable>[] axisValueSets,
        final GroupingSetsList groupingSetsList)
        throws SQLException {
        final List<Segment> segments = groupingSetsList.getDefaultSegments();
        final int measureCount = segments.size();
        final ResultSet rawRows = this.loadData(stmt, groupingSetsList);
        assert stmt != null;
        final List<SqlStatement.Type> types = stmt.guessTypes();
        final int arity = axisValueSets.length;
        final int groupingColumnStartIndex = arity + measureCount;

        // If we're using grouping sets, the SQL query will have a number of
        // indicator columns, and we roll these into a single BitSet column in
        // the processed data set.
        final List<SqlStatement.Type> processedTypes;
        if (groupingSetsList.useGroupingSets()) {
            processedTypes = new ArrayList<SqlStatement.Type>(types.subList(0, groupingColumnStartIndex));
            processedTypes.add(SqlStatement.Type.OBJECT);
        } else {
            processedTypes = types;
        }
        final RowList processedRows = new RowList(processedTypes, 100);

        final Execution execution = Locus.peek().execution;
        while (rawRows.next()) {
            // Check if the MDX query was canceled.
            CancellationChecker.checkCancelOrTimeout(++stmt.rowCount, execution);

            this.checkResultLimit(stmt.rowCount);
            processedRows.createRow();

            // get the columns
            int columnIndex = 0;
            for (int axisIndex = 0; axisIndex < arity; axisIndex++, columnIndex++) {
                final SqlStatement.Type type = types.get(columnIndex);
                switch (type) {
                    case OBJECT:
                    case STRING:
                    case DATE:
                    case TIMESTAMP:
                    case TIME:
                        Object o = rawRows.getObject(columnIndex + 1);
                        if (o == null) {
                            o = RolapUtil.sqlNullValue;
                            if (!groupingSetsList.useGroupingSets()
                                || !this.isAggregateNull(rawRows, groupingColumnStartIndex, groupingSetsList, axisIndex)) {
                                axisContainsNull[axisIndex] = true;
                            }
                        } else {
                            // We assume that all values are Comparable. Boolean
                            // wasn't Comparable until JDK 1.5, but we can live with
                            // that bug because JDK 1.4 is no longer important.

                            // byte [] is not Comparable.
                            // For our case it can be binary array. It was typed as String.
                            // So it can be processing (comparing and displaying) correctly as String
                            if (o instanceof byte[]) {
                                o = new String((byte[]) o);
                            }
                            axisValueSets[axisIndex].add((Comparable) o);
                        }
                        processedRows.setObject(columnIndex, o);
                        break;
                    case INT:
                        final int intValue = rawRows.getInt(columnIndex + 1);
                        if ((intValue == 0) && rawRows.wasNull()) {
                            if (!groupingSetsList.useGroupingSets()
                                || !this.isAggregateNull(rawRows, groupingColumnStartIndex, groupingSetsList, axisIndex)) {
                                axisContainsNull[axisIndex] = true;
                            }
                            processedRows.setNull(columnIndex, true);
                        } else {
                            axisValueSets[axisIndex].add(intValue);
                            processedRows.setInt(columnIndex, intValue);
                        }
                        break;
                    case LONG:
                        final long longValue = rawRows.getLong(columnIndex + 1);
                        if ((longValue == 0) && rawRows.wasNull()) {
                            if (!groupingSetsList.useGroupingSets()
                                || !this.isAggregateNull(rawRows, groupingColumnStartIndex, groupingSetsList, axisIndex)) {
                                axisContainsNull[axisIndex] = true;
                            }
                            processedRows.setNull(columnIndex, true);
                        } else {
                            axisValueSets[axisIndex].add(longValue);
                            processedRows.setLong(columnIndex, longValue);
                        }
                        break;
                    case DOUBLE:
                        final double doubleValue = rawRows.getDouble(columnIndex + 1);
                        if ((doubleValue == 0) && rawRows.wasNull()) {
                            if (!groupingSetsList.useGroupingSets()
                                || !this.isAggregateNull(rawRows, groupingColumnStartIndex, groupingSetsList, axisIndex)) {
                                axisContainsNull[axisIndex] = true;
                            }
                            processedRows.setNull(columnIndex, true);
                        } else {
                            axisValueSets[axisIndex].add(doubleValue);
                            processedRows.setDouble(columnIndex, doubleValue);
                        }
                        break;
                    default:
                        throw Util.unexpected(type);
                }
            }

            // pre-compute which measures are numeric
            final boolean[] numeric = new boolean[measureCount];
            int k = 0;
            for (final Segment segment : segments) {
                numeric[k++] = segment.measure.getDatatype().isNumeric();
            }

            // get the measure
            for (int i = 0; i < measureCount; i++, columnIndex++) {
                final SqlStatement.Type type = types.get(columnIndex);
                switch (type) {
                    case OBJECT:
                    case STRING:
                        Object o = rawRows.getObject(columnIndex + 1);
                        if (o == null) {
                            o = Util.nullValue; // convert to placeholder
                        } else if (numeric[i]) {
                            if (o instanceof java.lang.Double) {
                                // nothing to do
                            } else if (o instanceof java.math.BigDecimal) {
                                // nothing to do // PDI-16761 if we cast it to double type we lose precision
                            } else if (o instanceof java.lang.Number) {
                                o = ((Number) o).doubleValue();
                            } else if (o instanceof java.sql.Date) {
                                // nothing to do
                            } else if (o instanceof java.sql.Timestamp) {
                                // nothing to do
                            } else if (o instanceof java.sql.Time) {
                                // nothing to do
                            } else if (o instanceof byte[]) {
                                // On MySQL 5.0 in German locale, values can come
                                // out as byte arrays. Don't know why. Bug 1594119.
                                final java.lang.String oValue = new String((byte[]) o);
                                o = this.adjustObjectValue(oValue);
                            } else {
                                final java.lang.String oValue = o.toString();
                                o = this.adjustObjectValue(oValue);
                            }
                        }
                        processedRows.setObject(columnIndex, o);
                        break;
                    case DATE:
                        final java.sql.Date dateValue = rawRows.getDate(columnIndex + 1);
                        processedRows.setObject(columnIndex, dateValue);
                        if ((dateValue == null) && rawRows.wasNull()) {
                            processedRows.setNull(columnIndex, true);
                        }
                        break;
                    case TIMESTAMP:
                        final java.sql.Timestamp timestampValue = rawRows.getTimestamp(columnIndex + 1);
                        processedRows.setObject(columnIndex, timestampValue);
                        if ((timestampValue == null) && rawRows.wasNull()) {
                            processedRows.setNull(columnIndex, true);
                        }
                        break;
                    case TIME:
                        final java.sql.Time timeValue = rawRows.getTime(columnIndex + 1);
                        processedRows.setObject(columnIndex, timeValue);
                        if ((timeValue == null) && rawRows.wasNull()) {
                            processedRows.setNull(columnIndex, true);
                        }
                        break;

                    case INT:
                        final int intValue = rawRows.getInt(columnIndex + 1);
                        processedRows.setInt(columnIndex, intValue);
                        if ((intValue == 0) && rawRows.wasNull()) {
                            processedRows.setNull(columnIndex, true);
                        }
                        break;
                    case LONG:
                        final long longValue = rawRows.getLong(columnIndex + 1);
                        processedRows.setLong(columnIndex, longValue);
                        if ((longValue == 0) && rawRows.wasNull()) {
                            processedRows.setNull(columnIndex, true);
                        }
                        break;
                    case DOUBLE:
                        final double doubleValue = rawRows.getDouble(columnIndex + 1);
                        processedRows.setDouble(columnIndex, doubleValue);
                        if ((doubleValue == 0) && rawRows.wasNull()) {
                            processedRows.setNull(columnIndex, true);
                        }
                        break;
                    default:
                        throw Util.unexpected(type);
                }
            }

            if (groupingSetsList.useGroupingSets()) {
                processedRows
                    .setObject(columnIndex, this.getRollupBitKey(groupingSetsList.getRollupColumns().size(), rawRows, columnIndex));
            }
        }
        return processedRows;
    }

    private void checkResultLimit(int currentCount) {
        final int limit = MondrianProperties.instance().ResultLimit.get();
        if ((limit > 0) && (currentCount > limit)) {
            throw MondrianResource.instance().SegmentFetchLimitExceeded.ex(limit);
        }
    }

    /**
     * Generates bit key representing roll up columns
     */
    BitKey getRollupBitKey(int arity, ResultSet rowList, int k)
        throws SQLException {
        final BitKey groupingBitKey = BitKey.Factory.makeBitKey(arity);
        for (int i = 0; i < arity; i++) {
            final int o = rowList.getInt(k + i + 1);
            if (o == 1) {
                groupingBitKey.set(i);
            }
        }
        return groupingBitKey;
    }

    private boolean isAggregateNull(ResultSet rowList, int groupingColumnStartIndex, GroupingSetsList groupingSetsList, int axisIndex)
        throws SQLException {
        final int groupingFunctionIndex = groupingSetsList.findGroupingFunctionIndex(axisIndex);
        if (groupingFunctionIndex == -1) {
            // Not a rollup column
            return false;
        }
        return rowList.getInt(groupingColumnStartIndex + groupingFunctionIndex + 1) == 1;
    }

    ResultSet loadData(SqlStatement stmt, GroupingSetsList groupingSetsList)
        throws SQLException {
        final int arity = groupingSetsList.getDefaultColumns().length;
        final int measureCount = groupingSetsList.getDefaultSegments().size();
        final int groupingFunctionsCount = groupingSetsList.getRollupColumns().size();
        final List<SqlStatement.Type> types = stmt.guessTypes();
        assert (arity + measureCount + groupingFunctionsCount) == types.size();

        return stmt.getResultSet();
    }

    SortedSet<Comparable>[] getDistinctValueWorkspace(int arity) {
        // Workspace to build up lists of distinct values for each axis.
        final SortedSet<Comparable>[] axisValueSets = new SortedSet[arity];
        for (int i = 0; i < axisValueSets.length; i++) {
            axisValueSets[i] = new TreeSet<Comparable>();
        }
        return axisValueSets;
    }

    /**
     * Decides whether to use a sparse representation for this segment, using
     * the formula described
     * {@link mondrian.olap.MondrianProperties#SparseSegmentCountThreshold
     * here}.
     *
     * @param possibleCount Number of values in the space.
     * @param actualCount   Actual number of values.
     * @return Whether to use a sparse representation.
     */
    static boolean useSparse(final double possibleCount, final double actualCount) {
        final MondrianProperties properties = MondrianProperties.instance();
        double densityThreshold = properties.SparseSegmentDensityThreshold.get();
        if (densityThreshold < 0) {
            densityThreshold = 0;
        }
        if (densityThreshold > 1) {
            densityThreshold = 1;
        }
        int countThreshold = properties.SparseSegmentCountThreshold.get();
        if (countThreshold < 0) {
            countThreshold = 0;
        }
        final boolean sparse = ((possibleCount - countThreshold) * densityThreshold) > actualCount;
        if (possibleCount < countThreshold) {
            assert !sparse : "Should never use sparse if count is less " + "than threshold, possibleCount="
                + possibleCount
                + ", actualCount="
                + actualCount
                + ", countThreshold="
                + countThreshold
                + ", densityThreshold="
                + densityThreshold;
        }
        if (possibleCount == actualCount) {
            assert !sparse : "Should never use sparse if result is 100% dense: " + "possibleCount="
                + possibleCount
                + ", actualCount="
                + actualCount
                + ", countThreshold="
                + countThreshold
                + ", densityThreshold="
                + densityThreshold;
        }
        return sparse;
    }

    private java.lang.Object adjustObjectValue(java.lang.String oValue) {
        java.lang.Object o = null;

        // Konvertiert den Wert in ein Datum
        boolean flgConverted = false;
        try {
            o = java.sql.Date.valueOf(oValue);
            flgConverted = true;
        } catch (final java.lang.IllegalArgumentException ex) {
            try {
                o = new java.sql.Date(Timestamp.valueOf(oValue).getTime());
                flgConverted = true;
            } catch (final java.lang.IllegalArgumentException ex2) {
            }
        }
        if (flgConverted == false) {
            // Probiere es jetzt gegen einen Timestamp
            try {
                o = java.sql.Timestamp.valueOf(oValue);
                flgConverted = true;
            } catch (final java.lang.IllegalArgumentException ex) {
            }
        }
        if (flgConverted == false) {
            // Probiere es jetzt gegen einen Time
            try {
                o = java.sql.Time.valueOf(oValue);
                flgConverted = true;
            } catch (final java.lang.IllegalArgumentException ex) {
            }
        }

        if (flgConverted == false) {
            o = java.lang.Double.parseDouble(oValue);
        }

        return o;
    }

    /**
     * This is a private abstraction wrapper to perform
     * rollups. It allows us to rollup from a mix of segments
     * coming from either the local cache or the external one.
     */
    abstract class SegmentRollupWrapper {
        abstract BitKey getConstrainedColumnsBitKey();
        abstract SegmentColumn[] getConstrainedColumns();
        abstract SegmentDataset getDataset();
        abstract Object[] getValuesForColumn(SegmentColumn cc);
        abstract mondrian.spi.SegmentColumn getHeader();
        @Override
        public int hashCode() {
            return this.getHeader().hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return this.getHeader().equals(obj);
        }
    }

    /**
     * Collection of rows, each with a set of columns of type Object, double, or
     * int. Native types are not boxed.
     */
    protected static class RowList {
        private final Column[] columns;
        private int rowCount = 0;
        private int capacity = 0;
        private int currentRow = -1;

        /**
         * Creates a RowList.
         *
         * @param types Column types
         */
        RowList(List<SqlStatement.Type> types) {
            this(types, 100);
        }

        /**
         * Creates a RowList with a specified initial capacity.
         *
         * @param types Column types
         * @param capacity Initial capacity
         */
        RowList(List<SqlStatement.Type> types, int capacity) {
            this.columns = new Column[types.size()];
            this.capacity = capacity;
            for (int i = 0; i < this.columns.length; i++) {
                this.columns[i] = Column.forType(i, types.get(i), capacity);
            }
        }

        void createRow() {
            this.currentRow = this.rowCount++;
            if (this.rowCount > this.capacity) {
                this.capacity *= 3;
                for (final Column column : this.columns) {
                    column.resize(this.capacity);
                }
            }
        }

        void setObject(int column, Object value) {
            this.columns[column].setObject(this.currentRow, value);
        }

        void setDouble(int column, double value) {
            this.columns[column].setDouble(this.currentRow, value);
        }

        void setInt(int column, int value) {
            this.columns[column].setInt(this.currentRow, value);
        }

        void setLong(int column, long value) {
            this.columns[column].setLong(this.currentRow, value);
        }

        public int size() {
            return this.rowCount;
        }

        public void createRow(ResultSet resultSet)
            throws SQLException {
            this.createRow();
            for (final Column column : this.columns) {
                column.populateFrom(this.currentRow, resultSet);
            }
        }

        public List<SqlStatement.Type> getTypes() {
            return new AbstractList<SqlStatement.Type>() {
                @Override
                public SqlStatement.Type get(int index) {
                    return RowList.this.columns[index].type;
                }

                @Override
                public int size() {
                    return RowList.this.columns.length;
                }
            };
        }

        /**
         * Moves to before the first row.
         */
        public void first() {
            this.currentRow = -1;
        }

        /**
         * Moves to after the last row.
         */
        public void last() {
            this.currentRow = this.rowCount;
        }

        /**
         * Moves forward one row, or returns false if at the last row.
         *
         * @return whether moved forward
         */
        public boolean next() {
            if (this.currentRow < (this.rowCount - 1)) {
                ++this.currentRow;
                return true;
            }
            return false;
        }

        /**
         * Moves backward one row, or returns false if at the first row.
         *
         * @return whether moved backward
         */
        public boolean previous() {
            if (this.currentRow > 0) {
                --this.currentRow;
                return true;
            }
            return false;
        }

        /**
         * Returns the object in the given column of the current row.
         *
         * @param columnIndex Column index
         * @return Value of the column
         */
        public Object getObject(int columnIndex) {
            return this.columns[columnIndex].getObject(this.currentRow);
        }

        public int getInt(int columnIndex) {
            return this.columns[columnIndex].getInt(this.currentRow);
        }

        public double getDouble(int columnIndex) {
            return this.columns[columnIndex].getDouble(this.currentRow);
        }

        public boolean isNull(int columnIndex) {
            return this.columns[columnIndex].isNull(this.currentRow);
        }

        public void setNull(int columnIndex, boolean b) {
            this.columns[columnIndex].setNull(this.currentRow, b);
        }

        static abstract class Column {
            final int ordinal;
            final SqlStatement.Type type;

            protected Column(int ordinal, SqlStatement.Type type) {
                this.ordinal = ordinal;
                this.type = type;
            }

            static Column forType(int ordinal, mondrian.rolap.SqlStatement.Type type, int capacity) {
                switch (type) {
                    case OBJECT:
                    case STRING:
                    case DATE:
                    case TIMESTAMP:
                    case TIME:
                        return new ObjectColumn(ordinal, type, capacity);
                    case INT:
                        return new IntColumn(ordinal, type, capacity);
                    case LONG:
                        return new LongColumn(ordinal, type, capacity);
                    case DOUBLE:
                        return new DoubleColumn(ordinal, type, capacity);
                    default:
                        throw Util.unexpected(type);
                }
            }

            public abstract void resize(int newSize);

            public void setObject(int row, Object value) {
                throw new UnsupportedOperationException();
            }

            public void setDouble(int row, double value) {
                throw new UnsupportedOperationException();
            }

            public void setInt(int row, int value) {
                throw new UnsupportedOperationException();
            }

            public void setLong(int row, long value) {
                throw new UnsupportedOperationException();
            }

            public void setNull(int row, boolean b) {
                throw new UnsupportedOperationException();
            }

            public abstract void populateFrom(int row, ResultSet resultSet)
                throws SQLException;

            public Object getObject(int row) {
                throw new UnsupportedOperationException();
            }

            public int getInt(int row) {
                throw new UnsupportedOperationException();
            }

            public double getDouble(int row) {
                throw new UnsupportedOperationException();
            }

            protected abstract int getCapacity();

            public abstract boolean isNull(int row);
        }

        static class ObjectColumn extends Column {
            private Object[] objects;

            ObjectColumn(int ordinal, SqlStatement.Type type, int size) {
                super(ordinal, type);
                this.objects = new Object[size];
            }

            @Override
            protected int getCapacity() {
                return this.objects.length;
            }

            @Override
            public boolean isNull(int row) {
                return this.objects[row] == null;
            }

            @Override
            public void resize(int newSize) {
                this.objects = Util.copyOf(this.objects, newSize);
            }

            @Override
            public void populateFrom(int row, ResultSet resultSet)
                throws SQLException {
                this.objects[row] = resultSet.getObject(this.ordinal + 1);
            }

            @Override
            public void setObject(int row, Object value) {
                this.objects[row] = value;
            }

            @Override
            public Object getObject(int row) {
                return this.objects[row];
            }
        }

        static abstract class NativeColumn extends Column {
            protected BitSet nullIndicators;

            NativeColumn(int ordinal, SqlStatement.Type type) {
                super(ordinal, type);
            }

            @Override
            public void setNull(int row, boolean b) {
                this.getNullIndicators().set(row, b);
            }

            protected BitSet getNullIndicators() {
                if (this.nullIndicators == null) {
                    this.nullIndicators = new BitSet(this.getCapacity());
                }
                return this.nullIndicators;
            }
        }

        static class IntColumn extends NativeColumn {
            private int[] ints;

            IntColumn(int ordinal, SqlStatement.Type type, int size) {
                super(ordinal, type);
                this.ints = new int[size];
            }

            @Override
            public void resize(int newSize) {
                this.ints = Util.copyOf(this.ints, newSize);
            }

            @Override
            public void populateFrom(int row, ResultSet resultSet)
                throws SQLException {
                final int i = this.ints[row] = resultSet.getInt(this.ordinal + 1);
                if (i == 0) {
                    this.getNullIndicators().set(row, resultSet.wasNull());
                }
            }

            @Override
            public void setInt(int row, int value) {
                this.ints[row] = value;
            }

            @Override
            public int getInt(int row) {
                return this.ints[row];
            }

            @Override
            public boolean isNull(int row) {
                return (this.ints[row] == 0) && (this.nullIndicators != null) && this.nullIndicators.get(row);
            }

            @Override
            protected int getCapacity() {
                return this.ints.length;
            }

            @Override
            public Integer getObject(int row) {
                return this.isNull(row) ? null : this.ints[row];
            }
        }

        static class LongColumn extends NativeColumn {
            private long[] longs;

            LongColumn(int ordinal, SqlStatement.Type type, int size) {
                super(ordinal, type);
                this.longs = new long[size];
            }

            @Override
            public void resize(int newSize) {
                this.longs = Util.copyOf(this.longs, newSize);
            }

            @Override
            public void populateFrom(int row, ResultSet resultSet)
                throws SQLException {
                final long i = this.longs[row] = resultSet.getLong(this.ordinal + 1);
                if (i == 0) {
                    this.getNullIndicators().set(row, resultSet.wasNull());
                }
            }

            @Override
            public void setLong(int row, long value) {
                this.longs[row] = value;
            }

            public long getLong(int row) {
                return this.longs[row];
            }

            @Override
            public boolean isNull(int row) {
                return (this.longs[row] == 0) && (this.nullIndicators != null) && this.nullIndicators.get(row);
            }

            @Override
            protected int getCapacity() {
                return this.longs.length;
            }

            @Override
            public Long getObject(int row) {
                return this.isNull(row) ? null : this.longs[row];
            }
        }

        static class DoubleColumn extends NativeColumn {
            private double[] doubles;

            DoubleColumn(int ordinal, SqlStatement.Type type, int size) {
                super(ordinal, type);
                this.doubles = new double[size];
            }

            @Override
            public void resize(int newSize) {
                this.doubles = Util.copyOf(this.doubles, newSize);
            }

            @Override
            public void populateFrom(int row, ResultSet resultSet)
                throws SQLException {
                final double d = this.doubles[row] = resultSet.getDouble(this.ordinal + 1);
                if (d == 0d) {
                    this.getNullIndicators().set(row, resultSet.wasNull());
                }
            }

            @Override
            public void setDouble(int row, double value) {
                this.doubles[row] = value;
            }

            @Override
            public double getDouble(int row) {
                return this.doubles[row];
            }

            @Override
            protected int getCapacity() {
                return this.doubles.length;
            }

            @Override
            public boolean isNull(int row) {
                return (this.doubles[row] == 0d) && (this.nullIndicators != null) && this.nullIndicators.get(row);
            }

            @Override
            public Double getObject(int row) {
                return this.isNull(row) ? null : this.doubles[row];
            }
        }

        public interface Handler {
        }
    }

    private static class BooleanComparator implements Comparator<java.lang.Object>, java.io.Serializable {
        /**
         *
         */
        private static final long serialVersionUID = -8766078113343249123L;

        public static final BooleanComparator INSTANCE = new BooleanComparator();

        private BooleanComparator() {
            assert Comparable.class.isAssignableFrom(Boolean.class);
        }

        @Override
        public int compare(java.lang.Object o1, java.lang.Object o2) {
            if (o1 instanceof Boolean) {
                final boolean b1 = (Boolean) o1;
                if (o2 instanceof Boolean) {
                    final boolean b2 = (Boolean) o2;
                    return b1 == b2 ? 0 : (b1 ? 1 : -1);
                } else {
                    return -1;
                }
            } else {
                return ((Comparable) o1).compareTo(o2);
            }
        }
    }
}

// End SegmentLoader.java
