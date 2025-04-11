package com.memorydb.rest;

import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.distribution.ClusterManager;
import com.memorydb.query.Condition;
import com.memorydb.query.Query;
import com.memorydb.query.QueryExecutor;
import com.memorydb.query.QueryResult;
import com.memorydb.rest.dto.QueryDto;
import com.memorydb.rest.dto.QueryResultDto;
import com.memorydb.storage.ColumnStore;
import com.memorydb.storage.TableData;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource REST pour les requêtes
 */
@Path("/api/query")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {
    
    private static final Logger logger = LoggerFactory.getLogger(QueryResource.class);
    
    @Inject
    private DatabaseContext databaseContext;
    
    @Inject
    private ClusterManager clusterManager;
    
    @Inject
    private QueryExecutor queryExecutor;
    
    /**
     * Exécute une requête distribuée ou locale avec une implémentation simplifiée
     * @param queryDto La requête à exécuter
     * @return Les résultats de la requête
     */
    @POST
    public Response executeQuery(QueryDto queryDto) {
        try {
            // Vérifie que le nom de la table est valide
            if (queryDto.getTableName() == null || queryDto.getTableName().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le nom de la table est obligatoire")
                        .build();
            }
            
            // Vérifie que la table existe
            String tableName = queryDto.getTableName();
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            // Si le paramètre 'distributed' est true, utilise l'implémentation distribuée
            if (queryDto.isDistributed()) {
                // Convertit le DTO en objet Query pour l'exécution distribuée
                Query query = convertDtoToQuery(queryDto);
                List<Map<String, Object>> results = clusterManager.executeDistributedQuery(query);
                return Response.ok(results).build();
            }
            
            // Implémentation simplifiée pour l'exécution locale (contourne le QueryExecutor)
            Table table = databaseContext.getTable(tableName);
            TableData tableData = databaseContext.getTableData(tableName);
            
            // Prépare les colonnes à sélectionner
            List<String> selectedColumns = queryDto.getColumns();
            if (selectedColumns == null || selectedColumns.isEmpty()) {
                // Si aucune colonne n'est spécifiée, on sélectionne toutes les colonnes
                selectedColumns = new ArrayList<>();
                for (Column column : table.getColumns()) {
                    selectedColumns.add(column.getName());
                }
            } else if (selectedColumns.size() == 1 && selectedColumns.get(0).equals("*")) {
                // Si on a utilisé l'astérisque, on sélectionne toutes les colonnes
                selectedColumns = new ArrayList<>();
                for (Column column : table.getColumns()) {
                    selectedColumns.add(column.getName());
                }
            }
            
            // Vérifie que les colonnes existent
            for (String columnName : selectedColumns) {
                if (table.getColumnIndex(columnName) == -1) {
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity("Colonne inconnue: " + columnName)
                            .build();
                }
            }
            
            // Récupère les conditions
            List<Condition> conditions = queryDto.toConditions();
            
            // Acquiert un verrou en lecture
            tableData.readLock();
            try {
                // Filtre les lignes en fonction des conditions
                List<Integer> filteredRows = new ArrayList<>();
                int rowCount = tableData.getRowCount();
                
                // Parcourt toutes les lignes
                for (int i = 0; i < rowCount; i++) {
                    boolean match = true;
                    
                    // Vérifie toutes les conditions
                    if (conditions != null) {
                        for (Condition condition : conditions) {
                            String columnName = condition.getColumnName();
                            ColumnStore columnStore = tableData.getColumnStore(columnName);
                            
                            // Évalue la condition
                            try {
                                if (!evaluateCondition(condition, i, columnStore)) {
                                    match = false;
                                    break;
                                }
                            } catch (Exception e) {
                                logger.error("Erreur lors de l'évaluation de la condition sur la colonne {}: {}", 
                                        columnName, e.getMessage());
                                match = false;
                                break;
                            }
                        }
                    }
                    
                    // Si toutes les conditions sont satisfaites, ajoute l'index de la ligne
                    if (match) {
                        filteredRows.add(i);
                    }
                }
                
                // Applique la limite
                int limit = queryDto.getLimit();
                if (limit > 0 && filteredRows.size() > limit) {
                    filteredRows = filteredRows.subList(0, limit);
                }
                
                // Construit le résultat
                List<Map<String, Object>> resultRows = new ArrayList<>();
                
                // Récupère les valeurs pour chaque ligne filtrée
                for (int rowIndex : filteredRows) {
                    Map<String, Object> row = new HashMap<>();
                    
                    // Récupère les valeurs pour chaque colonne sélectionnée
                    for (String columnName : selectedColumns) {
                        ColumnStore columnStore = tableData.getColumnStore(columnName);
                        Object value = extractColumnValue(rowIndex, columnStore);
                        row.put(columnName, value);
                    }
                    
                    resultRows.add(row);
                }
                
                return Response.ok(resultRows).build();
            } finally {
                tableData.readUnlock();
            }
        } catch (Exception e) {
            logger.error("Erreur lors de l'exécution de la requête: {}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de l'exécution de la requête: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Exécute une requête simplifiée (non distribuée) avec accès direct aux données
     */
    @GET
    @Path("/{tableName}")
    public Response executeSimpleQuery(
            @PathParam("tableName") String tableName,
            @QueryParam("column") String column,
            @QueryParam("value") String value,
            @QueryParam("limit") @DefaultValue("100") int limit) {
        try {
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            Table table = databaseContext.getTable(tableName);
            TableData tableData = databaseContext.getTableData(tableName);
            
            // Acquiert un verrou en lecture
            tableData.readLock();
            try {
                // Récupère toutes les colonnes
                List<String> columnNames = new ArrayList<>();
                for (Column col : table.getColumns()) {
                    columnNames.add(col.getName());
                }
                
                // Filtre les lignes en fonction de la condition
                List<Integer> filteredRows = new ArrayList<>();
                int rowCount = tableData.getRowCount();
                
                // Parcourt toutes les lignes
                for (int i = 0; i < rowCount; i++) {
                    boolean match = true;
                    
                    // Si une condition est spécifiée, l'évalue
                    if (column != null && !column.isEmpty() && value != null) {
                        try {
                            ColumnStore columnStore = tableData.getColumnStore(column);
                            if (columnStore.isNull(i)) {
                                match = false;
                            } else {
                                // Comparaison simplifiée convertissant la valeur en chaîne
                                Object rowValue = extractColumnValue(i, columnStore);
                                if (rowValue == null || !rowValue.toString().equals(value)) {
                                    match = false;
                                }
                            }
                        } catch (Exception e) {
                            match = false;
                        }
                    }
                    
                    // Si la condition est satisfaite, ajoute l'index de la ligne
                    if (match) {
                        filteredRows.add(i);
                    }
                }
                
                // Applique la limite
                if (limit > 0 && filteredRows.size() > limit) {
                    filteredRows = filteredRows.subList(0, limit);
                }
                
                // Construit le résultat
                List<Map<String, Object>> resultRows = new ArrayList<>();
                
                // Récupère les valeurs pour chaque ligne filtrée
                for (int rowIndex : filteredRows) {
                    Map<String, Object> row = new HashMap<>();
                    
                    // Récupère les valeurs pour chaque colonne
                    for (String columnName : columnNames) {
                        ColumnStore columnStore = tableData.getColumnStore(columnName);
                        Object value2 = extractColumnValue(rowIndex, columnStore);
                        row.put(columnName, value2);
                    }
                    
                    resultRows.add(row);
                }
                
                return Response.ok(resultRows).build();
            } finally {
                tableData.readUnlock();
            }
        } catch (Exception e) {
            logger.error("Erreur lors de l'exécution de la requête: {}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de l'exécution de la requête: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Extrait la valeur d'une cellule en fonction du type de colonne
     */
    private Object extractColumnValue(int rowIndex, ColumnStore columnStore) {
        if (columnStore.isNull(rowIndex)) {
            return null;
        }
        
        try {
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
                    return null;
            }
        } catch (Exception e) {
            logger.error("Erreur lors de l'extraction de la valeur: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Évalue une condition sur une ligne
     */
    private boolean evaluateCondition(Condition condition, int rowIndex, ColumnStore columnStore) {
        // Si la valeur est null, traitement spécial pour IS_NULL et IS_NOT_NULL
        if (columnStore.isNull(rowIndex)) {
            if (condition.getOperator() == Condition.Operator.IS_NULL) {
                return true;
            } else if (condition.getOperator() == Condition.Operator.IS_NOT_NULL) {
                return false;
            } else {
                return false; // Toute autre condition sur une valeur null est fausse
            }
        }
        
        // Si opérateur IS_NULL, c'est faux car la valeur n'est pas null
        if (condition.getOperator() == Condition.Operator.IS_NULL) {
            return false;
        }
        
        // Si opérateur IS_NOT_NULL, c'est vrai car la valeur n'est pas null
        if (condition.getOperator() == Condition.Operator.IS_NOT_NULL) {
            return true;
        }
        
        // Récupère la valeur de la cellule
        Object cellValue = extractColumnValue(rowIndex, columnStore);
        if (cellValue == null) {
            return false;
        }
        
        // Récupère la valeur de condition
        Object conditionValue = condition.getValue();
        if (conditionValue == null) {
            return false;
        }
        
        // Évalue la condition en fonction du type
        switch (columnStore.getType()) {
            case INTEGER:
                return evaluateIntCondition((Integer) cellValue, conditionValue, condition.getOperator());
            case LONG:
                return evaluateLongCondition((Long) cellValue, conditionValue, condition.getOperator());
            case FLOAT:
                return evaluateFloatCondition((Float) cellValue, conditionValue, condition.getOperator());
            case DOUBLE:
                return evaluateDoubleCondition((Double) cellValue, conditionValue, condition.getOperator());
            case BOOLEAN:
                return evaluateBooleanCondition((Boolean) cellValue, conditionValue, condition.getOperator());
            case STRING:
                return evaluateStringCondition((String) cellValue, conditionValue, condition.getOperator());
            case DATE:
            case TIMESTAMP:
                return evaluateLongCondition((Long) cellValue, conditionValue, condition.getOperator());
            default:
                return false;
        }
    }
    
    private boolean evaluateIntCondition(Integer cellValue, Object conditionValue, Condition.Operator operator) {
        int value = ((Number) conditionValue).intValue();
        
        switch (operator) {
            case EQUALS:
                return cellValue == value;
            case NOT_EQUALS:
                return cellValue != value;
            case LESS_THAN:
                return cellValue < value;
            case LESS_THAN_OR_EQUALS:
                return cellValue <= value;
            case GREATER_THAN:
                return cellValue > value;
            case GREATER_THAN_OR_EQUALS:
                return cellValue >= value;
            default:
                return false;
        }
    }
    
    private boolean evaluateLongCondition(Long cellValue, Object conditionValue, Condition.Operator operator) {
        long value = ((Number) conditionValue).longValue();
        
        switch (operator) {
            case EQUALS:
                return cellValue == value;
            case NOT_EQUALS:
                return cellValue != value;
            case LESS_THAN:
                return cellValue < value;
            case LESS_THAN_OR_EQUALS:
                return cellValue <= value;
            case GREATER_THAN:
                return cellValue > value;
            case GREATER_THAN_OR_EQUALS:
                return cellValue >= value;
            default:
                return false;
        }
    }
    
    private boolean evaluateFloatCondition(Float cellValue, Object conditionValue, Condition.Operator operator) {
        float value = ((Number) conditionValue).floatValue();
        
        switch (operator) {
            case EQUALS:
                return cellValue == value;
            case NOT_EQUALS:
                return cellValue != value;
            case LESS_THAN:
                return cellValue < value;
            case LESS_THAN_OR_EQUALS:
                return cellValue <= value;
            case GREATER_THAN:
                return cellValue > value;
            case GREATER_THAN_OR_EQUALS:
                return cellValue >= value;
            default:
                return false;
        }
    }
    
    private boolean evaluateDoubleCondition(Double cellValue, Object conditionValue, Condition.Operator operator) {
        double value = ((Number) conditionValue).doubleValue();
        
        switch (operator) {
            case EQUALS:
                return cellValue == value;
            case NOT_EQUALS:
                return cellValue != value;
            case LESS_THAN:
                return cellValue < value;
            case LESS_THAN_OR_EQUALS:
                return cellValue <= value;
            case GREATER_THAN:
                return cellValue > value;
            case GREATER_THAN_OR_EQUALS:
                return cellValue >= value;
            default:
                return false;
        }
    }
    
    private boolean evaluateBooleanCondition(Boolean cellValue, Object conditionValue, Condition.Operator operator) {
        boolean value = (Boolean) conditionValue;
        
        switch (operator) {
            case EQUALS:
                return cellValue == value;
            case NOT_EQUALS:
                return cellValue != value;
            default:
                return false;
        }
    }
    
    private boolean evaluateStringCondition(String cellValue, Object conditionValue, Condition.Operator operator) {
        String value = conditionValue.toString();
        
        switch (operator) {
            case EQUALS:
                return cellValue.equals(value);
            case NOT_EQUALS:
                return !cellValue.equals(value);
            case LIKE:
                // Implémentation simplifiée de LIKE: % remplace n'importe quelle séquence de caractères
                String pattern = value.replace("%", ".*");
                return cellValue.matches(pattern);
            default:
                return false;
        }
    }
    
    /**
     * Convertit un DTO de requête en objet Query pour l'exécution distribuée
     */
    private Query convertDtoToQuery(QueryDto queryDto) {
        Query query = new Query(queryDto.getTableName());
        
        // Ajoute les colonnes
        List<String> columns = queryDto.getColumns();
        if (columns != null) {
            for (String column : columns) {
                query.select(column);
            }
        }
        
        // Ajoute les conditions
        List<Condition> conditions = queryDto.toConditions();
        if (conditions != null) {
            for (Condition condition : conditions) {
                query.where(condition);
            }
        }
        
        // Ajoute le tri
        if (queryDto.getOrderBy() != null && !queryDto.getOrderBy().isEmpty()) {
            if (queryDto.isOrderByAscending()) {
                query.orderByAsc(queryDto.getOrderBy());
            } else {
                query.orderByDesc(queryDto.getOrderBy());
            }
        }
        
        // Ajoute la limite
        if (queryDto.getLimit() > 0) {
            query.limit(queryDto.getLimit());
        }
        
        return query;
    }
} 