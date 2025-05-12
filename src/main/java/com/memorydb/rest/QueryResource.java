package com.memorydb.rest;

import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.distribution.ClusterManager;
import com.memorydb.query.Condition;
import com.memorydb.query.Query;

import com.memorydb.rest.dto.OrderByDto;
import com.memorydb.rest.dto.QueryDto;
import com.memorydb.rest.dto.QueryResultDto;
import com.memorydb.storage.ColumnStore;
import com.memorydb.storage.TableData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                List<Integer> filteredRows;
                int rowCount = tableData.getRowCount();
                
                // Utilise les index bitmap si disponibles pour une exécution plus rapide
                if (conditions != null && !conditions.isEmpty()) {
                    // Essaie d'utiliser les index bitmap pour des performances optimales
                    BitSet matchingRows = findMatchingRowsWithIndexes(tableData, conditions, rowCount);
                    
                    // Convertit le BitSet en liste d'entiers
                    if (matchingRows != null) {
                        // L'optimisation des index a fonctionné
                        // Optimisation: utiliser des ArrayList pour réduire les allocations
                        int cardinality = matchingRows.cardinality();
                        filteredRows = new ArrayList<>(cardinality);
                        for (int i = matchingRows.nextSetBit(0); i >= 0; i = matchingRows.nextSetBit(i+1)) {
                            filteredRows.add(i);
                        }
                    } else {
                        // Revient à l'approche classique (scan complet)
                        filteredRows = findMatchingRowsWithFullScan(tableData, conditions, rowCount);
                    }
                } else {
                    // Pas de conditions, inclut toutes les lignes
                    // Optimisation: pré-allouer avec la taille exacte
                    filteredRows = new ArrayList<>(rowCount);
                    for (int i = 0; i < rowCount; i++) {
                        filteredRows.add(i);
                    }
                }
                
                // Applique le tri (ORDER BY)
                List<OrderByDto> orderByList = queryDto.getOrderBy();
                if (orderByList != null && !orderByList.isEmpty()) {
                    // On crée des comparateurs pour chaque critère de tri
                    filteredRows.sort((row1, row2) -> {
                        // Parcourt les critères de tri dans l'ordre
                        for (OrderByDto orderBy : orderByList) {
                            String columnName = orderBy.getColumn();
                            boolean ascending = orderBy.isAscending();
                            
                            // Récupère les colonnes de données
                            ColumnStore columnStore = tableData.getColumnStore(columnName);
                            
                            // Compare les valeurs
                            int comparison = compareValues(columnStore, row1, row2);
                            
                            // Si les valeurs sont différentes, on retourne le résultat de la comparaison
                            if (comparison != 0) {
                                return ascending ? comparison : -comparison; // Inverse pour l'ordre descendant
                            }
                            
                            // Si les valeurs sont égales, on continue avec le critère suivant
                        }
                        
                        // Si toutes les valeurs sont égales, on retourne 0
                        return 0;
                    });
                } else if (queryDto.getOrderByColumn() != null && !queryDto.getOrderByColumn().isEmpty()) {
                    // Support de l'ancien format avec une seule colonne
                    String columnName = queryDto.getOrderByColumn();
                    boolean ascending = queryDto.isOrderByAscending();
                    
                    // Récupère la colonne de données
                    ColumnStore columnStore = tableData.getColumnStore(columnName);
                    
                    // Trie les lignes selon la colonne spécifiée
                    filteredRows.sort((row1, row2) -> {
                        int comparison = compareValues(columnStore, row1, row2);
                        return ascending ? comparison : -comparison; // Inverse pour l'ordre descendant
                    });
                }
                
                // Applique la limite
                int limit = queryDto.getLimit();
                if (limit > 0 && filteredRows.size() > limit) {
                    filteredRows = filteredRows.subList(0, limit);
                }
                
                // Construit le résultat en respectant l'ordre des colonnes demandées
                QueryResultDto resultDto = new QueryResultDto();
                
                // Définit l'ordre des colonnes dans le résultat (important pour préserver l'ordre demandé)
                resultDto.setColumns(selectedColumns);
                
                int resultCount = Math.min(filteredRows.size(), limit);
                
                // Optimisation: utiliser directement un tableau 2D au lieu d'une liste de Maps
                Object[][] resultData = new Object[resultCount][selectedColumns.size()];
                
                // Remplir le tableau de résultats dans l'ordre demandé
                for (int i = 0; i < resultCount; i++) {
                    int rowIndex = filteredRows.get(i);
                    
                    for (int j = 0; j < selectedColumns.size(); j++) {
                        String columnName = selectedColumns.get(j);
                        ColumnStore columnStore = tableData.getColumnStore(columnName);
                        resultData[i][j] = extractColumnValue(rowIndex, columnStore);
                    }
                }
                
                // Utiliser la version optimisée du DTO avec stockage en tableau
                resultDto.setRawData(resultData);
                
                // IMPORTANT: On renvoie maintenant un DTO qui garantit l'ordre des colonnes
                return Response.ok(resultDto).build();
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
     * Compare deux valeurs d'une colonne pour le tri
     * @param columnStore Le stockage de colonne
     * @param rowIndex1 L'index de la première ligne
     * @param rowIndex2 L'index de la deuxième ligne
     * @return La comparaison des valeurs
     */
    private int compareValues(ColumnStore columnStore, int rowIndex1, int rowIndex2) {
        // Vérifie si les valeurs sont nulles
        boolean isNull1 = columnStore.isNull(rowIndex1);
        boolean isNull2 = columnStore.isNull(rowIndex2);
        
        // Les valeurs nulles sont toujours considérées comme plus petites
        if (isNull1 && isNull2) return 0;
        if (isNull1) return -1;
        if (isNull2) return 1;
        
        // Compare les valeurs en fonction du type de la colonne
        switch (columnStore.getType()) {
            case INTEGER:
                return Integer.compare(columnStore.getInt(rowIndex1), columnStore.getInt(rowIndex2));
                
            case LONG:
                return Long.compare(columnStore.getLong(rowIndex1), columnStore.getLong(rowIndex2));
                
            case FLOAT:
                return Float.compare(columnStore.getFloat(rowIndex1), columnStore.getFloat(rowIndex2));
                
            case DOUBLE:
                return Double.compare(columnStore.getDouble(rowIndex1), columnStore.getDouble(rowIndex2));
                
            case BOOLEAN:
                return Boolean.compare(columnStore.getBoolean(rowIndex1), columnStore.getBoolean(rowIndex2));
                
            case STRING:
                String str1 = columnStore.getString(rowIndex1);
                String str2 = columnStore.getString(rowIndex2);
                return str1.compareTo(str2);
                
            case DATE:
            case TIMESTAMP:
                // Comparaison de dates directement (assuming getDate returns long timestamp)
                return Long.compare(
                    columnStore.getLong(rowIndex1), 
                    columnStore.getLong(rowIndex2));
                
            default:
                // En cas de type inconnu, on considère les valeurs comme égales
                return 0;
        }
    }
    
    /**
     * Trouve les lignes correspondant aux conditions en utilisant les index bitmap
     * @param tableData Les données de la table
     * @param conditions Les conditions à évaluer
     * @param rowCount Le nombre total de lignes
     * @return BitSet des lignes correspondantes ou null si les index ne peuvent pas être utilisés
     */
    private BitSet findMatchingRowsWithIndexes(TableData tableData, List<Condition> conditions, int rowCount) {
        // Résultat initial: toutes les lignes sont considérées comme correspondantes
        BitSet result = new BitSet(rowCount);
        result.set(0, rowCount);
        
        // Marque pour indiquer si au moins un index a été utilisé
        boolean usedIndex = false;
        
        for (Condition condition : conditions) {
            String columnName = condition.getColumnName();
            ColumnStore columnStore = tableData.getColumnStore(columnName);
            
            // Vérifie si la colonne est indexée et si la condition est une condition d'égalité
            if (columnStore.getColumn().isIndexed() && condition.getOperator() == Condition.Operator.EQUALS) {
                Object value = condition.getValue();
                BitSet matchingForCondition = columnStore.findEqual(value);
                
                if (matchingForCondition != null) {
                    // Intersection avec le résultat actuel (AND logique)
                    result.and(matchingForCondition);
                    usedIndex = true;
                } else {
                    // Si indexé mais ne peut pas utiliser l'index pour cette valeur, on continue avec les autres conditions
                    continue;
                }
            } else {
                // La condition n'est pas une condition d'égalité ou la colonne n'est pas indexée
                continue;
            }
        }
        
        // Si aucun index n'a été utilisé, on retourne null pour indiquer qu'il faut utiliser l'approche classique
        if (!usedIndex) {
            return null;
        }
        
        // On doit maintenant vérifier les autres conditions qui n'utilisent pas d'index
        BitSet finalResult = new BitSet(rowCount);
        
        // Parcourt les lignes qui correspondent aux conditions indexées
        for (int i = result.nextSetBit(0); i >= 0; i = result.nextSetBit(i+1)) {
            boolean match = true;
            
            // Vérifie les conditions non indexées
            for (Condition condition : conditions) {
                String columnName = condition.getColumnName();
                ColumnStore columnStore = tableData.getColumnStore(columnName);
                
                // Saute les conditions qui ont déjà été traitées par les index
                if (columnStore.getColumn().isIndexed() && condition.getOperator() == Condition.Operator.EQUALS) {
                    continue;
                }
                
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
            
            if (match) {
                finalResult.set(i);
            }
        }
        
        return finalResult;
    }
    
    /**
     * Trouve les lignes correspondant aux conditions en utilisant un scan complet 
     * @param tableData Les données de la table
     * @param conditions Les conditions à évaluer
     * @param rowCount Le nombre total de lignes
     * @return Liste des indices de lignes correspondantes
     */
    private List<Integer> findMatchingRowsWithFullScan(TableData tableData, List<Condition> conditions, int rowCount) {
        List<Integer> filteredRows = new ArrayList<>();
        
        // Parcourt toutes les lignes
        for (int i = 0; i < rowCount; i++) {
            boolean match = true;
            
            // Vérifie toutes les conditions
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
            
            // Si toutes les conditions sont satisfaites, ajoute l'index de la ligne
            if (match) {
                filteredRows.add(i);
            }
        }
        
        return filteredRows;
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
        
        // Ajoute le tri (support des colonnes multiples)
        List<OrderByDto> orderByList = queryDto.getOrderBy();
        if (orderByList != null && !orderByList.isEmpty()) {
            // Pour le premier élément, utiliser orderByAsc/orderByDesc pour maintenir la compatibilité
            OrderByDto firstOrderBy = orderByList.get(0);
            if (firstOrderBy.isAscending()) {
                query.orderByAsc(firstOrderBy.getColumn());
            } else {
                query.orderByDesc(firstOrderBy.getColumn());
            }
            
            // Pour les éléments suivants, utiliser addOrderByAsc/addOrderByDesc
            for (int i = 1; i < orderByList.size(); i++) {
                OrderByDto orderBy = orderByList.get(i);
                if (orderBy.isAscending()) {
                    query.addOrderByAsc(orderBy.getColumn());
                } else {
                    query.addOrderByDesc(orderBy.getColumn());
                }
            }
        } else if (queryDto.getOrderByColumn() != null && !queryDto.getOrderByColumn().isEmpty()) {
            // Compatibilité descendante
            if (queryDto.isOrderByAscending()) {
                query.orderByAsc(queryDto.getOrderByColumn());
            } else {
                query.orderByDesc(queryDto.getOrderByColumn());
            }
        }
        
        // Ajoute la limite
        if (queryDto.getLimit() > 0) {
            query.limit(queryDto.getLimit());
        }
        
        return query;
    }
} 