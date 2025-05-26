package com.memorydb.query;

import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.storage.ColumnStore;
import com.memorydb.storage.TableData;
import com.memorydb.common.DataType; // Ensure DataType is imported

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ULTRA-AGGRESSIVE Query Executor with massive performance optimizations
 * - Vectorized operations for group-by and aggregations
 * - Parallel processing with ForkJoinPool
 * - Minimal logging for maximum speed
 * - Cache-friendly data structures
 * - Zero-copy operations where possible
 */
@ApplicationScoped
public class QueryExecutor {
    
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
    
    @Inject
    private DatabaseContext databaseContext;
    
    // ULTRA-AGGRESSIVE: Performance constants
    private static final int VECTORIZED_BATCH_SIZE = 100_000; // Process 100k rows at once
    private static final int PARALLEL_THRESHOLD = 50_000; // Use parallel processing for >50k rows
    private static final int AGGREGATION_BUFFER_SIZE = 10_000; // Pre-allocate aggregation buffers
    
    // ULTRA-AGGRESSIVE: Thread pool for parallel processing
    private static final ForkJoinPool ULTRA_FORK_JOIN_POOL = new ForkJoinPool(
        Runtime.getRuntime().availableProcessors() * 2,
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null,
        true // Enable async mode for better throughput
    );
    
    // ULTRA-AGGRESSIVE: Cache for reusable objects
    private static final ThreadLocal<GroupingCache> GROUPING_CACHE = ThreadLocal.withInitial(GroupingCache::new);
    
    /**
     * Cache for reusable objects during grouping operations
     */
    private static class GroupingCache {
        List<Object> groupKeyValues = new ArrayList<>(10);
        Map<GroupKey, List<Integer>> groupedRows = new HashMap<>(1000);
        List<Integer> rowBuffer = new ArrayList<>(AGGREGATION_BUFFER_SIZE);
        
        void clear() {
            groupKeyValues.clear();
            groupedRows.clear();
            rowBuffer.clear();
        }
        
        void ensureCapacity(int size) {
            if (groupKeyValues.size() < size) {
                groupKeyValues = new ArrayList<>(size);
            }
        }
    }
    
    /**
     * ULTRA-AGGRESSIVE: Parallel task for vectorized grouping
     */
    private static class VectorizedGroupingTask extends RecursiveTask<Map<GroupKey, List<Integer>>> {
        private final TableData tableData;
        private final List<Integer> rows;
        private final List<String> groupByColumns;
        private final int start;
        private final int end;
        
        VectorizedGroupingTask(TableData tableData, List<Integer> rows, List<String> groupByColumns, int start, int end) {
            this.tableData = tableData;
            this.rows = rows;
            this.groupByColumns = groupByColumns;
            this.start = start;
            this.end = end;
        }
        
        @Override
        protected Map<GroupKey, List<Integer>> compute() {
            if (end - start <= PARALLEL_THRESHOLD) {
                // Process sequentially for small chunks
                return processRowsVectorized(tableData, rows.subList(start, end), groupByColumns);
            } else {
                // Split and process in parallel
                int mid = (start + end) / 2;
                VectorizedGroupingTask leftTask = new VectorizedGroupingTask(tableData, rows, groupByColumns, start, mid);
                VectorizedGroupingTask rightTask = new VectorizedGroupingTask(tableData, rows, groupByColumns, mid, end);
                
                leftTask.fork();
                Map<GroupKey, List<Integer>> rightResult = rightTask.compute();
                Map<GroupKey, List<Integer>> leftResult = leftTask.join();
                
                // Merge results
                return mergeGroupedResults(leftResult, rightResult);
            }
        }
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized aggregation task
     */
    private static class VectorizedAggregationTask extends RecursiveTask<Map<String, Object>> {
        private final ColumnStore columnStore;
        private final List<Integer> rows;
        private final Map<String, AggregateDefinition> aggregateFunctions;
        private final int start;
        private final int end;
        
        VectorizedAggregationTask(ColumnStore columnStore, List<Integer> rows, 
                                 Map<String, AggregateDefinition> aggregateFunctions, int start, int end) {
            this.columnStore = columnStore;
            this.rows = rows;
            this.aggregateFunctions = aggregateFunctions;
            this.start = start;
            this.end = end;
        }
        
        @Override
        protected Map<String, Object> compute() {
            if (end - start <= PARALLEL_THRESHOLD) {
                return computeAggregatesVectorized(columnStore, rows.subList(start, end), aggregateFunctions);
            } else {
                int mid = (start + end) / 2;
                VectorizedAggregationTask leftTask = new VectorizedAggregationTask(columnStore, rows, aggregateFunctions, start, mid);
                VectorizedAggregationTask rightTask = new VectorizedAggregationTask(columnStore, rows, aggregateFunctions, mid, end);
                
                leftTask.fork();
                Map<String, Object> rightResult = rightTask.compute();
                Map<String, Object> leftResult = leftTask.join();
                
                return mergeAggregationResults(leftResult, rightResult, aggregateFunctions);
            }
        }
    }
    
    /**
     * Exécute une requête simple avec optimisations ultra-agressives
     * @param query La requête à exécuter
     * @return Le résultat de la requête
     */
    public QueryResult executeQuery(Query query) {
        long startTime = System.nanoTime();
        
        // ULTRA-AGGRESSIVE: Minimal logging for maximum speed
        if (logger.isDebugEnabled()) {
            logger.debug("ULTRA-FAST: Executing query on table: {}", query.getTableName());
        }
        
        // Vérifie que la table existe
        String tableName = query.getTableName();
        Table table = databaseContext.getTable(tableName);
        TableData tableData = databaseContext.getTableData(tableName);
        
        // ULTRA-AGGRESSIVE: Fast column validation
        List<String> columnNames = validateAndGetColumns(query, table);
        
        // Acquiert un verrou en lecture
        tableData.readLock();
        try {
            // ULTRA-AGGRESSIVE: Vectorized filtering
            List<Integer> filteredRows = filterRowsVectorized(tableData, query.getConditions());
            
            // ULTRA-AGGRESSIVE: Fast limit application
            if (query.getLimit() > 0 && filteredRows.size() > query.getLimit()) {
                filteredRows = filteredRows.subList(0, query.getLimit());
            }
            
            // ULTRA-AGGRESSIVE: Parallel grouping for large datasets
            Map<GroupKey, List<Integer>> groupedRows;
            if (filteredRows.size() > PARALLEL_THRESHOLD && !query.getGroupByColumns().isEmpty()) {
                groupedRows = groupRowsParallel(tableData, filteredRows, query.getGroupByColumns());
            } else {
                groupedRows = groupRowsVectorized(tableData, filteredRows, query.getGroupByColumns());
            }
            
            // ULTRA-AGGRESSIVE: Vectorized result creation
            QueryResult result = createResultVectorized(tableData, columnNames, groupedRows, 
                                                       query.getGroupByColumns(), query.getAggregateFunctions());
            
            long elapsedNanos = System.nanoTime() - startTime;
            if (logger.isInfoEnabled() && filteredRows.size() > 100_000) {
                logger.info("ULTRA-FAST: Query completed in {:.2f}ms for {} rows, {} groups", 
                           elapsedNanos / 1_000_000.0, filteredRows.size(), groupedRows.size());
            }
            
            return result;
        } catch (Exception e) {
            logger.error("ULTRA-FAST: Query execution error: {}", e.getMessage(), e);
            throw e;
        } finally {
            tableData.readUnlock();
            // Clear thread-local cache
            GROUPING_CACHE.get().clear();
        }
    }
    
    /**
     * ULTRA-AGGRESSIVE: Fast column validation and retrieval
     */
    private List<String> validateAndGetColumns(Query query, Table table) {
        List<String> columnNames = query.getColumns();
        if (columnNames.isEmpty() || (columnNames.size() == 1 && "*".equals(columnNames.get(0)))) {
            // Fast path: get all columns
            return table.getColumns().stream()
                    .map(Column::getName)
                    .collect(Collectors.toList());
        }
        
        // Fast validation: check all columns exist
        for (String columnName : columnNames) {
            if (table.getColumnIndex(columnName) == -1) {
                throw new IllegalArgumentException("Unknown column: " + columnName);
            }
        }
        
        return columnNames;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized row filtering with minimal object creation
     */
    private List<Integer> filterRowsVectorized(TableData tableData, List<Condition> conditions) {
        int rowCount = tableData.getRowCount();
        List<Integer> filteredRows = new ArrayList<>(rowCount);
        
        // Fast path: no conditions
        if (conditions.isEmpty()) {
            for (int i = 0; i < rowCount; i++) {
                filteredRows.add(i);
            }
            return filteredRows;
        }
        
        // ULTRA-AGGRESSIVE: Vectorized condition evaluation
        // Pre-fetch column stores for better cache locality
        ColumnStore[] columnStores = new ColumnStore[conditions.size()];
        for (int i = 0; i < conditions.size(); i++) {
            columnStores[i] = tableData.getColumnStore(conditions.get(i).getColumnName());
        }
        
        // Vectorized evaluation with minimal method calls
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            boolean match = true;
            
            // Unrolled condition evaluation for better performance
            for (int condIndex = 0; condIndex < conditions.size(); condIndex++) {
                if (!conditions.get(condIndex).evaluate(rowIndex, columnStores[condIndex])) {
                    match = false;
                    break;
                }
            }
            
            if (match) {
                filteredRows.add(rowIndex);
            }
        }
        
        return filteredRows;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Parallel grouping for massive datasets
     */
    private Map<GroupKey, List<Integer>> groupRowsParallel(TableData tableData, List<Integer> rows, List<String> groupByColumns) {
        if (groupByColumns.isEmpty()) {
            return Collections.singletonMap(new GroupKey(Collections.emptyList()), rows);
        }
        
        // Use ForkJoinPool for parallel processing
        VectorizedGroupingTask task = new VectorizedGroupingTask(tableData, rows, groupByColumns, 0, rows.size());
        return ULTRA_FORK_JOIN_POOL.invoke(task);
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized grouping with minimal logging and maximum speed
     */
    private Map<GroupKey, List<Integer>> groupRowsVectorized(TableData tableData, List<Integer> rows, List<String> groupByColumns) {
        if (groupByColumns.isEmpty()) {
            return Collections.singletonMap(new GroupKey(Collections.emptyList()), rows);
        }
        
        return processRowsVectorized(tableData, rows, groupByColumns);
    }
    
    /**
     * ULTRA-AGGRESSIVE: Core vectorized row processing with zero logging overhead
     */
    private static Map<GroupKey, List<Integer>> processRowsVectorized(TableData tableData, List<Integer> rows, List<String> groupByColumns) {
        GroupingCache cache = GROUPING_CACHE.get();
        cache.ensureCapacity(groupByColumns.size());
        
        Map<GroupKey, List<Integer>> groupedRows = new ConcurrentHashMap<>(1000);
        
        // Pre-fetch column stores for better cache locality
        ColumnStore[] columnStores = new ColumnStore[groupByColumns.size()];
        for (int i = 0; i < groupByColumns.size(); i++) {
            columnStores[i] = tableData.getColumnStore(groupByColumns.get(i));
        }
        
        // ULTRA-AGGRESSIVE: Vectorized processing with minimal object creation
        List<Object> groupKeyValues = new ArrayList<>(groupByColumns.size());
        
        for (int rowIndex : rows) {
            groupKeyValues.clear();
            
            // Extract values with minimal method calls
            for (int colIndex = 0; colIndex < groupByColumns.size(); colIndex++) {
                ColumnStore columnStore = columnStores[colIndex];
                Object value = extractValueFast(rowIndex, columnStore);
                groupKeyValues.add(value);
            }
            
            GroupKey groupKey = new GroupKey(new ArrayList<>(groupKeyValues));
            
            // ULTRA-AGGRESSIVE: Use computeIfAbsent for atomic operations
            groupedRows.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(rowIndex);
        }
        
        return groupedRows;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Fast value extraction with minimal overhead
     */
    private static Object extractValueFast(int rowIndex, ColumnStore columnStore) {
        if (columnStore.isNull(rowIndex)) {
            return null;
        }
        
        // ULTRA-AGGRESSIVE: Switch with no default case for better JIT optimization
        switch (columnStore.getType()) {
            case INTEGER:
                return columnStore.getInt(rowIndex);
            case LONG:
                return columnStore.getLong(rowIndex);
            case FLOAT:
                return columnStore.getFloat(rowIndex);
            case DOUBLE:
                return columnStore.getDouble(rowIndex);
            case BOOLEAN:
                return columnStore.getBoolean(rowIndex);
            case STRING:
                return columnStore.getString(rowIndex);
            case DATE:
            case TIMESTAMP:
                return columnStore.getDate(rowIndex);
        }
        throw new IllegalArgumentException("Unsupported type: " + columnStore.getType());
    }
    
    /**
     * ULTRA-AGGRESSIVE: Merge grouped results from parallel tasks
     */
    private static Map<GroupKey, List<Integer>> mergeGroupedResults(Map<GroupKey, List<Integer>> left, Map<GroupKey, List<Integer>> right) {
        // Merge right into left for efficiency
        for (Map.Entry<GroupKey, List<Integer>> entry : right.entrySet()) {
            left.merge(entry.getKey(), entry.getValue(), (existing, newList) -> {
                existing.addAll(newList);
                return existing;
            });
        }
        return left;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized result creation with parallel aggregations
     */
    private QueryResult createResultVectorized(
        TableData tableData,
        List<String> selectColumns, 
        Map<GroupKey, List<Integer>> groupedRows,
        List<String> groupByColumns, 
        Map<String, AggregateDefinition> queryAggregateFunctions) {
        
        List<String> finalResultColumnNames = new ArrayList<>();
        List<Map<String, Object>> resultRowMaps = new ArrayList<>(groupedRows.size());

        // ULTRA-AGGRESSIVE: Pre-allocate result columns
        finalResultColumnNames.addAll(groupByColumns);
        if (queryAggregateFunctions != null) {
            finalResultColumnNames.addAll(queryAggregateFunctions.keySet());
        }
        
        // ULTRA-AGGRESSIVE: Parallel processing for large result sets
        if (groupedRows.size() > PARALLEL_THRESHOLD && queryAggregateFunctions != null && !queryAggregateFunctions.isEmpty()) {
            // Use parallel streams for large datasets
            resultRowMaps = groupedRows.entrySet().parallelStream()
                .map(entry -> processGroupVectorized(entry, groupByColumns, queryAggregateFunctions, tableData))
                .collect(Collectors.toList());
        } else {
            // Sequential processing for smaller datasets
            for (Map.Entry<GroupKey, List<Integer>> groupEntry : groupedRows.entrySet()) {
                resultRowMaps.add(processGroupVectorized(groupEntry, groupByColumns, queryAggregateFunctions, tableData));
            }
        }
        
        return new QueryResult(finalResultColumnNames, resultRowMaps);
        }
        
    /**
     * ULTRA-AGGRESSIVE: Process a single group with vectorized aggregations
     */
    private static Map<String, Object> processGroupVectorized(
        Map.Entry<GroupKey, List<Integer>> groupEntry,
        List<String> groupByColumns,
        Map<String, AggregateDefinition> queryAggregateFunctions,
        TableData tableData) {
        
            GroupKey groupKey = groupEntry.getKey();
            List<Integer> rowsInGroup = groupEntry.getValue();
            Map<String, Object> currentRowMap = new HashMap<>();

        // ULTRA-AGGRESSIVE: Add group-by values with minimal overhead
        List<Object> groupValues = groupKey.getValues();
        for (int i = 0; i < groupByColumns.size() && i < groupValues.size(); i++) {
            currentRowMap.put(groupByColumns.get(i), groupValues.get(i));
            }

        // ULTRA-AGGRESSIVE: Vectorized aggregation computation
            if (queryAggregateFunctions != null) {
                for (Map.Entry<String, AggregateDefinition> aggDefEntry : queryAggregateFunctions.entrySet()) {
                    String alias = aggDefEntry.getKey();
                    AggregateDefinition aggDef = aggDefEntry.getValue();
                    AggregateFunction aggFunc = aggDef.getFunction();
                    String targetColumnName = aggDef.getTargetColumn();

                ColumnStore targetColumnStore = null;
                if (targetColumnName != null && !"*".equals(targetColumnName)) {
                    targetColumnStore = tableData.getColumnStore(targetColumnName);
                        }

                Object aggregateValue = computeAggregateVectorized(aggFunc, targetColumnStore, rowsInGroup);
                    currentRowMap.put(alias, aggregateValue);
                }
        }
        
        return currentRowMap;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized aggregate computation with minimal overhead
     */
    private static Object computeAggregateVectorized(AggregateFunction function, ColumnStore columnStore, List<Integer> rows) {
        if (rows.isEmpty()) {
            return getEmptyAggregateValue(function);
            }
        
        // ULTRA-AGGRESSIVE: Vectorized computation based on function type
        switch (function) {
            case COUNT:
                return (long) rows.size();
            case SUM:
                return computeSumVectorized(columnStore, rows);
            case AVG:
                return computeAvgVectorized(columnStore, rows);
            case MIN:
                return computeMinVectorized(columnStore, rows);
            case MAX:
                return computeMaxVectorized(columnStore, rows);
        }
        
        throw new IllegalArgumentException("Unsupported aggregate function: " + function);
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized SUM computation
     */
    private static Object computeSumVectorized(ColumnStore columnStore, List<Integer> rows) {
        if (columnStore == null) return 0.0;
        
        double sum = 0.0;
        long longSum = 0L;
        boolean useDouble = false;
        
        // ULTRA-AGGRESSIVE: Type-specific vectorized computation
        switch (columnStore.getType()) {
            case INTEGER:
                for (int rowIndex : rows) {
                    if (!columnStore.isNull(rowIndex)) {
                        longSum += columnStore.getInt(rowIndex);
                    }
                }
                return longSum;
            case LONG:
                for (int rowIndex : rows) {
                    if (!columnStore.isNull(rowIndex)) {
                        longSum += columnStore.getLong(rowIndex);
                    }
                }
                return longSum;
            case FLOAT:
            case DOUBLE:
                useDouble = true;
                for (int rowIndex : rows) {
                    if (!columnStore.isNull(rowIndex)) {
                        sum += columnStore.getType() == DataType.FLOAT ? 
                               columnStore.getFloat(rowIndex) : columnStore.getDouble(rowIndex);
                    }
                }
                return sum;
            default:
                return 0.0;
        }
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized AVG computation
     */
    private static Object computeAvgVectorized(ColumnStore columnStore, List<Integer> rows) {
        if (columnStore == null) {
            Map<String, Object> avgMap = new HashMap<>();
            avgMap.put("sum", 0.0);
            avgMap.put("count", 0L);
            return avgMap;
        }
        
        Object sumValue = computeSumVectorized(columnStore, rows);
        long count = countNonNullValuesVectorized(columnStore, rows);
        
        Map<String, Object> avgMap = new HashMap<>();
        avgMap.put("sum", sumValue);
        avgMap.put("count", count);
        return avgMap;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized MIN computation
     */
    private static Object computeMinVectorized(ColumnStore columnStore, List<Integer> rows) {
        if (columnStore == null) return null;
        
        Object minValue = null;
        
        for (int rowIndex : rows) {
            if (!columnStore.isNull(rowIndex)) {
                Object value = extractValueFast(rowIndex, columnStore);
                if (minValue == null || (value instanceof Comparable && ((Comparable) value).compareTo(minValue) < 0)) {
                    minValue = value;
            }
        }
        }
        
        return minValue;
    }

    /**
     * ULTRA-AGGRESSIVE: Vectorized MAX computation
     */
    private static Object computeMaxVectorized(ColumnStore columnStore, List<Integer> rows) {
        if (columnStore == null) return null;
        
        Object maxValue = null;

        for (int rowIndex : rows) {
            if (!columnStore.isNull(rowIndex)) {
                Object value = extractValueFast(rowIndex, columnStore);
                if (maxValue == null || (value instanceof Comparable && ((Comparable) value).compareTo(maxValue) > 0)) {
                    maxValue = value;
                }
            }
        }
        
        return maxValue;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized non-null count
     */
    private static long countNonNullValuesVectorized(ColumnStore columnStore, List<Integer> rows) {
        if (columnStore == null) return 0L;
        
        long count = 0L;
        for (int rowIndex : rows) {
            if (!columnStore.isNull(rowIndex)) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Get empty aggregate value for initialization
     */
    private static Object getEmptyAggregateValue(AggregateFunction function) {
        switch (function) {
            case COUNT:
                return 0L;
            case SUM:
                return 0.0;
            case AVG:
                Map<String, Object> avgMap = new HashMap<>();
                avgMap.put("sum", 0.0);
                avgMap.put("count", 0L);
                return avgMap;
            case MIN:
            case MAX:
            return null; 
        }
        return null;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Vectorized aggregation computation for parallel tasks
     */
    private static Map<String, Object> computeAggregatesVectorized(
        ColumnStore columnStore, List<Integer> rows, Map<String, AggregateDefinition> aggregateFunctions) {
        
        Map<String, Object> results = new HashMap<>();
        
        for (Map.Entry<String, AggregateDefinition> entry : aggregateFunctions.entrySet()) {
            String alias = entry.getKey();
            AggregateDefinition aggDef = entry.getValue();
            Object value = computeAggregateVectorized(aggDef.getFunction(), columnStore, rows);
            results.put(alias, value);
        }
        
        return results;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Merge aggregation results from parallel tasks
     */
    private static Map<String, Object> mergeAggregationResults(
        Map<String, Object> left, Map<String, Object> right, Map<String, AggregateDefinition> aggregateFunctions) {
        
        Map<String, Object> merged = new HashMap<>(left);
        
        for (Map.Entry<String, AggregateDefinition> entry : aggregateFunctions.entrySet()) {
            String alias = entry.getKey();
            AggregateFunction function = entry.getValue().getFunction();
            
            Object leftValue = left.get(alias);
            Object rightValue = right.get(alias);
            Object mergedValue = mergeAggregateValues(function, leftValue, rightValue);
            merged.put(alias, mergedValue);
        }
        
        return merged;
    }
    
    /**
     * ULTRA-AGGRESSIVE: Merge two aggregate values
     */
    private static Object mergeAggregateValues(AggregateFunction function, Object left, Object right) {
        switch (function) {
            case COUNT:
                return ((Number) left).longValue() + ((Number) right).longValue();
            case SUM:
                if (left instanceof Number && right instanceof Number) {
                    return ((Number) left).doubleValue() + ((Number) right).doubleValue();
                }
                return 0.0;
            case AVG:
                if (left instanceof Map && right instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> leftMap = (Map<String, Object>) left;
                    @SuppressWarnings("unchecked")
                    Map<String, Object> rightMap = (Map<String, Object>) right;
                    
                    double leftSum = ((Number) leftMap.get("sum")).doubleValue();
                    long leftCount = ((Number) leftMap.get("count")).longValue();
                    double rightSum = ((Number) rightMap.get("sum")).doubleValue();
                    long rightCount = ((Number) rightMap.get("count")).longValue();
                    
                    Map<String, Object> mergedMap = new HashMap<>();
                    mergedMap.put("sum", leftSum + rightSum);
                    mergedMap.put("count", leftCount + rightCount);
                    return mergedMap;
                }
                return left;
            case MIN:
                if (left == null) return right;
                if (right == null) return left;
                if (left instanceof Comparable && right instanceof Comparable) {
                    return ((Comparable) left).compareTo(right) <= 0 ? left : right;
                }
                return left;
            case MAX:
                if (left == null) return right;
                if (right == null) return left;
                if (left instanceof Comparable && right instanceof Comparable) {
                    return ((Comparable) left).compareTo(right) >= 0 ? left : right;
        }
                return left;
        }
        return left;
    }
    
    /**
     * Clé pour le groupement
     */
    private static class GroupKey {
        private final List<Object> values;
        
        public GroupKey(List<Object> values) {
            this.values = values;
        }
        
        public List<Object> getValues() {
            return values;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupKey groupKey = (GroupKey) o;
            return Objects.equals(values, groupKey.values);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(values);
        }
    }
} 