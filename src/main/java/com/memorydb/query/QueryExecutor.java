package com.memorydb.query;

import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.storage.ColumnStore;
import com.memorydb.storage.TableData;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exécuteur de requêtes
 */
@ApplicationScoped
public class QueryExecutor {
    
    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
    
    @Inject
    private DatabaseContext databaseContext;
    
    /**
     * Exécute une requête simple
     * @param query La requête à exécuter
     * @return Le résultat de la requête
     */
    public QueryResult executeQuery(Query query) {
        logger.info("Exécution de la requête sur la table: {}", query.getTableName());
        // Vérifie que la table existe
        String tableName = query.getTableName();
        Table table = databaseContext.getTable(tableName);
        TableData tableData = databaseContext.getTableData(tableName);
        
        // Vérifie que les colonnes existent
        List<String> columnNames = query.getColumns();
        if (columnNames.isEmpty()) {
            // Si aucune colonne n'est spécifiée, on sélectionne toutes les colonnes
            columnNames = table.getColumns().stream()
                    .map(Column::getName)
                    .collect(Collectors.toList());
        } else if (columnNames.size() == 1 && columnNames.get(0).equals("*")) {
            // Si on a spécifié "*", on sélectionne toutes les colonnes
            columnNames = table.getColumns().stream()
                    .map(Column::getName)
                    .collect(Collectors.toList());
            logger.info("Remplacement de * par toutes les colonnes: {}", columnNames);
        } else {
            // Vérifie que toutes les colonnes existent
            for (String columnName : columnNames) {
                if (table.getColumnIndex(columnName) == -1) {
                    throw new IllegalArgumentException("Colonne inconnue: " + columnName);
                }
            }
        }
        
        logger.info("Colonnes sélectionnées: {}", columnNames);
        logger.info("Nombre de lignes dans la table: {}", tableData.getRowCount());
        
        // Acquiert un verrou en lecture
        tableData.readLock();
        try {
            // Filtre les lignes
            List<Integer> filteredRows = filterRows(tableData, query.getConditions());
            logger.info("Nombre de lignes après filtrage: {}", filteredRows.size());
            
            // Applique la limite si spécifiée
            if (query.getLimit() > 0 && filteredRows.size() > query.getLimit()) {
                logger.info("Application de la limite: {}", query.getLimit());
                filteredRows = filteredRows.subList(0, query.getLimit());
            }
            
            // Groupe les lignes si nécessaire
            Map<GroupKey, List<Integer>> groupedRows = groupRows(tableData, filteredRows, query.getGroupByColumns());
            logger.info("Nombre de groupes après regroupement: {}", groupedRows.size());
            
            // Prépare le résultat
            QueryResult result = createResult(tableData, columnNames, groupedRows, query.getGroupByColumns(), query.getAggregateFunctions());
            logger.info("Nombre de lignes dans le résultat: {}", result.getRowCount());
            return result;
        } catch (Exception e) {
            logger.error("Erreur lors de l'exécution de la requête: {}", e.getMessage(), e);
            throw e;
        } finally {
            tableData.readUnlock();
        }
    }
    
    /**
     * Filtre les lignes selon les conditions
     * @param tableData Les données de la table
     * @param conditions Les conditions
     * @return Les index des lignes filtrées
     */
    private List<Integer> filterRows(TableData tableData, List<Condition> conditions) {
        List<Integer> filteredRows = new ArrayList<>();
        
        // Si pas de conditions, on prend toutes les lignes
        if (conditions.isEmpty()) {
            for (int i = 0; i < tableData.getRowCount(); i++) {
                filteredRows.add(i);
            }
            return filteredRows;
        }
        
        // Évalue les conditions pour chaque ligne
        for (int i = 0; i < tableData.getRowCount(); i++) {
            boolean match = true;
            
            for (Condition condition : conditions) {
                String columnName = condition.getColumnName();
                ColumnStore columnStore = tableData.getColumnStore(columnName);
                
                if (!condition.evaluate(i, columnStore)) {
                    match = false;
                    break;
                }
            }
            
            if (match) {
                filteredRows.add(i);
            }
        }
        
        return filteredRows;
    }
    
    /**
     * Groupe les lignes selon les colonnes GROUP BY
     * @param tableData Les données de la table
     * @param rows Les index des lignes
     * @param groupByColumns Les colonnes de groupement
     * @return Les lignes groupées
     */
    private Map<GroupKey, List<Integer>> groupRows(TableData tableData, List<Integer> rows, List<String> groupByColumns) {
        // Si pas de GROUP BY, on crée un seul groupe avec toutes les lignes
        if (groupByColumns.isEmpty()) {
            return Collections.singletonMap(new GroupKey(Collections.emptyList()), rows);
        }
        
        // Groupe les lignes par les valeurs des colonnes GROUP BY
        Map<GroupKey, List<Integer>> groupedRows = new HashMap<>();
        
        for (int rowIndex : rows) {
            List<Object> groupKeyValues = new ArrayList<>(groupByColumns.size());
            
            for (String columnName : groupByColumns) {
                ColumnStore columnStore = tableData.getColumnStore(columnName);
                Object value = extractValue(rowIndex, columnStore);
                groupKeyValues.add(value);
            }
            
            GroupKey groupKey = new GroupKey(groupKeyValues);
            
            groupedRows.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(rowIndex);
        }
        
        return groupedRows;
    }
    
    /**
     * Extrait une valeur d'un ColumnStore
     * @param rowIndex L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return La valeur
     */
    private Object extractValue(int rowIndex, ColumnStore columnStore) {
        if (columnStore.isNull(rowIndex)) {
            return null;
        }
        
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
            default:
                throw new IllegalArgumentException("Type non supporté: " + columnStore.getType());
        }
    }
    
    /**
     * Crée le résultat de la requête
     * @param tableData Les données de la table
     * @param selectColumns Les colonnes à sélectionner
     * @param groupedRows Les lignes groupées
     * @param groupByColumns Les colonnes de groupement
     * @param aggregateFunctions Les fonctions d'agrégation
     * @return Le résultat de la requête
     */
    private QueryResult createResult(
            TableData tableData,
            List<String> selectColumns,
            Map<GroupKey, List<Integer>> groupedRows,
            List<String> groupByColumns,
            Map<String, AggregateFunction> aggregateFunctions) {
        
        List<String> resultColumns = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        
        // Ajoute les colonnes de groupement aux colonnes de résultat
        for (String groupByColumn : groupByColumns) {
            if (!resultColumns.contains(groupByColumn)) {
                resultColumns.add(groupByColumn);
            }
        }
        
        // Ajoute les colonnes sélectionnées aux colonnes de résultat
        for (String selectColumn : selectColumns) {
            // Si la colonne a une fonction d'agrégation, on l'ajoute avec le nom de la fonction
            AggregateFunction function = aggregateFunctions.get(selectColumn);
            if (function != null) {
                String columnName = function.getName() + "(" + selectColumn + ")";
                if (!resultColumns.contains(columnName)) {
                    resultColumns.add(columnName);
                }
            } else if (!resultColumns.contains(selectColumn)) {
                // Sinon, on l'ajoute telle quelle
                resultColumns.add(selectColumn);
            }
        }
        
        // Traite chaque groupe
        for (Map.Entry<GroupKey, List<Integer>> entry : groupedRows.entrySet()) {
            GroupKey groupKey = entry.getKey();
            List<Integer> groupRows = entry.getValue();
            
            Map<String, Object> resultRow = new HashMap<>();
            
            // Ajoute les valeurs des colonnes de groupement
            for (int i = 0; i < groupByColumns.size(); i++) {
                String columnName = groupByColumns.get(i);
                Object value = (groupKey.getValues().size() > i) ? groupKey.getValues().get(i) : null;
                resultRow.put(columnName, value);
            }
            
            // Traite les colonnes sélectionnées
            for (String selectColumn : selectColumns) {
                // Si la colonne a une fonction d'agrégation, on applique la fonction
                AggregateFunction function = aggregateFunctions.get(selectColumn);
                if (function != null) {
                    String resultColumnName = function.getName() + "(" + selectColumn + ")";
                    ColumnStore columnStore = tableData.getColumnStore(selectColumn);
                    Object aggregateValue = computeAggregate(function, columnStore, groupRows);
                    resultRow.put(resultColumnName, aggregateValue);
                } else if (groupByColumns.contains(selectColumn)) {
                    // Si la colonne est dans le GROUP BY, on a déjà ajouté sa valeur
                    continue;
                } else if (groupRows.size() == 1) {
                    // Si le groupe contient une seule ligne, on peut afficher la valeur directement
                    ColumnStore columnStore = tableData.getColumnStore(selectColumn);
                    Object value = extractValue(groupRows.get(0), columnStore);
                    resultRow.put(selectColumn, value);
                } else {
                    // Sinon, on ne peut pas afficher la valeur (nécessite un agrégat)
                    throw new IllegalArgumentException(
                        "La colonne '" + selectColumn + "' doit être dans la clause GROUP BY " +
                        "ou utilisée avec une fonction d'agrégation");
                }
            }
            
            rows.add(resultRow);
        }
        
        return new QueryResult(resultColumns, rows);
    }
    
    /**
     * Calcule une valeur agrégée
     * @param function La fonction d'agrégation
     * @param columnStore Le stockage de colonne
     * @param rows Les index des lignes
     * @return La valeur agrégée
     */
    private Object computeAggregate(AggregateFunction function, ColumnStore columnStore, List<Integer> rows) {
        if (rows.isEmpty()) {
            return null;
        }
        
        switch (function) {
            case COUNT:
                return (long) rows.size();
                
            case SUM:
                return computeSum(columnStore, rows);
                
            case AVG:
                Object sum = computeSum(columnStore, rows);
                if (sum instanceof Number) {
                    double numericSum = ((Number) sum).doubleValue();
                    return numericSum / rows.size();
                }
                return null;
                
            case MIN:
                return computeMin(columnStore, rows);
                
            case MAX:
                return computeMax(columnStore, rows);
                
            default:
                throw new IllegalArgumentException("Fonction d'agrégation non supportée: " + function);
        }
    }
    
    /**
     * Calcule la somme des valeurs
     * @param columnStore Le stockage de colonne
     * @param rows Les index des lignes
     * @return La somme
     */
    private Object computeSum(ColumnStore columnStore, List<Integer> rows) {
        switch (columnStore.getType()) {
            case INTEGER:
                int intSum = 0;
                for (int row : rows) {
                    if (!columnStore.isNull(row)) {
                        intSum += columnStore.getInt(row);
                    }
                }
                return intSum;
                
            case LONG:
                long longSum = 0L;
                for (int row : rows) {
                    if (!columnStore.isNull(row)) {
                        longSum += columnStore.getLong(row);
                    }
                }
                return longSum;
                
            case FLOAT:
                float floatSum = 0.0f;
                for (int row : rows) {
                    if (!columnStore.isNull(row)) {
                        floatSum += columnStore.getFloat(row);
                    }
                }
                return floatSum;
                
            case DOUBLE:
                double doubleSum = 0.0;
                for (int row : rows) {
                    if (!columnStore.isNull(row)) {
                        doubleSum += columnStore.getDouble(row);
                    }
                }
                return doubleSum;
                
            default:
                throw new IllegalArgumentException("Type non supporté pour SUM: " + columnStore.getType());
        }
    }
    
    /**
     * Calcule le minimum des valeurs
     * @param columnStore Le stockage de colonne
     * @param rows Les index des lignes
     * @return Le minimum
     */
    private Object computeMin(ColumnStore columnStore, List<Integer> rows) {
        // Implémentation simplifiée pour les types courants
        switch (columnStore.getType()) {
            case INTEGER:
                return rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .mapToInt(row -> columnStore.getInt(row))
                    .min()
                    .orElse(0);
                
            case LONG:
                return rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .mapToLong(row -> columnStore.getLong(row))
                    .min()
                    .orElse(0L);
                
            case FLOAT:
                OptionalDouble floatMin = rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .mapToDouble(row -> columnStore.getFloat(row))
                    .min();
                return floatMin.isPresent() ? (float) floatMin.getAsDouble() : 0.0f;
                
            case DOUBLE:
                return rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .mapToDouble(row -> columnStore.getDouble(row))
                    .min()
                    .orElse(0.0);
                
            case STRING:
                return rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .map(row -> columnStore.getString(row))
                    .min(String::compareTo)
                    .orElse("");
                
            default:
                throw new IllegalArgumentException("Type non supporté pour MIN: " + columnStore.getType());
        }
    }
    
    /**
     * Calcule le maximum des valeurs
     * @param columnStore Le stockage de colonne
     * @param rows Les index des lignes
     * @return Le maximum
     */
    private Object computeMax(ColumnStore columnStore, List<Integer> rows) {
        // Implémentation simplifiée pour les types courants
        switch (columnStore.getType()) {
            case INTEGER:
                return rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .mapToInt(row -> columnStore.getInt(row))
                    .max()
                    .orElse(0);
                
            case LONG:
                return rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .mapToLong(row -> columnStore.getLong(row))
                    .max()
                    .orElse(0L);
                
            case FLOAT:
                OptionalDouble floatMax = rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .mapToDouble(row -> columnStore.getFloat(row))
                    .max();
                return floatMax.isPresent() ? (float) floatMax.getAsDouble() : 0.0f;
                
            case DOUBLE:
                return rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .mapToDouble(row -> columnStore.getDouble(row))
                    .max()
                    .orElse(0.0);
                
            case STRING:
                return rows.stream()
                    .filter(row -> !columnStore.isNull(row))
                    .map(row -> columnStore.getString(row))
                    .max(String::compareTo)
                    .orElse("");
                
            default:
                throw new IllegalArgumentException("Type non supporté pour MAX: " + columnStore.getType());
        }
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