package com.memorydb.distribution;

import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.query.AggregateFunction;
import com.memorydb.query.Query;
import com.memorydb.query.AggregateDefinition;
import com.memorydb.query.QueryExecutor;
import com.memorydb.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Classe qui gère la distribution des données et la communication entre les nœuds du cluster
 */
@ApplicationScoped
public class ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    
    @Inject
    private DatabaseContext databaseContext;
    
    @Inject
    private QueryExecutor queryExecutor;
    
    @Inject
    private NodeDiscovery nodeDiscovery;
    
    private final Map<String, NodeInfo> clusterNodes = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private NodeInfo localNode;
    
    /**
     * Initialise le gestionnaire de cluster
     */
    public void initialize(String nodeId, String nodeAddress, int nodePort) {
        this.localNode = new NodeInfo(nodeId, nodeAddress, nodePort);
        
        // Ajoute le nœud local à la liste des nœuds
        clusterNodes.put(nodeId, localNode);
        
        // Démarre la découverte des nœuds
        nodeDiscovery.startDiscovery(this);
        
        logger.info("ClusterManager initialisé avec l'ID de nœud: {}", nodeId);
    }
    
    /**
     * Ajoute un nœud au cluster
     * @param nodeInfo Les informations du nœud
     */
    public void addNode(NodeInfo nodeInfo) {
        if (!clusterNodes.containsKey(nodeInfo.getId())) {
            clusterNodes.put(nodeInfo.getId(), nodeInfo);
            logger.info("Nouveau nœud ajouté au cluster: {}", nodeInfo);
            
            // Synchronise les tables existantes avec le nouveau nœud
            executorService.submit(() -> synchronizeTablesWithNode(nodeInfo));
        }
    }
    
    /**
     * Supprime un nœud du cluster
     * @param nodeId L'ID du nœud
     */
    public void removeNode(String nodeId) {
        if (clusterNodes.containsKey(nodeId)) {
            clusterNodes.remove(nodeId);
            logger.info("Nœud supprimé du cluster: {}", nodeId);
            
            // Rééquilibre les données si nécessaire
            executorService.submit(this::rebalanceData);
        }
    }
    
    /**
     * Obtient tous les nœuds du cluster
     * @return La liste des nœuds
     */
    public Collection<NodeInfo> getAllNodes() {
        return Collections.unmodifiableCollection(clusterNodes.values());
    }
    
    /**
     * Obtient le nœud local
     * @return Le nœud local
     */
    public NodeInfo getLocalNode() {
        return localNode;
    }
    
    /**
     * Synchronise la création d'une table avec tous les nœuds du cluster
     * @param tableName Le nom de la table
     * @param columns Les colonnes de la table
     */
    public void syncTableCreation(String tableName, List<Column> columns) {
        logger.info("Synchronisation de la création de la table '{}' avec tous les nœuds", tableName);
        
        // Pour chaque nœud (sauf le nœud local), envoie une demande de création de table
        for (NodeInfo node : getAllNodes()) {
            if (!node.getId().equals(localNode.getId())) {
                executorService.submit(() -> {
                    try {
                        // Utilise le client HTTP pour envoyer la demande
                        boolean success = NodeClient.sendTableCreationRequest(node, tableName, columns);
                        if (success) {
                            logger.info("Table '{}' créée avec succès sur le nœud {}", tableName, node.getId());
                        } else {
                            logger.error("Échec de la création de la table '{}' sur le nœud {}", tableName, node.getId());
                        }
                    } catch (Exception e) {
                        logger.error("Erreur lors de la synchronisation de la table '{}' avec le nœud {}: {}", 
                                tableName, node.getId(), e.getMessage());
                    }
                });
            }
        }
    }
    
    /**
     * Synchronise la suppression d'une table avec tous les nœuds du cluster
     * @param tableName Le nom de la table
     */
    public void syncTableDeletion(String tableName) {
        logger.info("Synchronisation de la suppression de la table '{}' avec tous les nœuds", tableName);
        
        // Pour chaque nœud (sauf le nœud local), envoie une demande de suppression de table
        for (NodeInfo node : getAllNodes()) {
            if (!node.getId().equals(localNode.getId())) {
                executorService.submit(() -> {
                    try {
                        // Utilise le client HTTP pour envoyer la demande
                        boolean success = NodeClient.sendTableDeletionRequest(node, tableName);
                        if (success) {
                            logger.info("Table '{}' supprimée avec succès sur le nœud {}", tableName, node.getId());
                        } else {
                            logger.error("Échec de la suppression de la table '{}' sur le nœud {}", tableName, node.getId());
                        }
                    } catch (Exception e) {
                        logger.error("Erreur lors de la synchronisation de la suppression de la table '{}' avec le nœud {}: {}", 
                                tableName, node.getId(), e.getMessage());
                    }
                });
            }
        }
    }
    
    /**
     * Exécute une requête distribuée sur tous les nœuds du cluster
     * @param query La requête à exécuter
     * @param queryDto Le DTO de la requête (contient des flags supplémentaires)
     * @return Les résultats agrégés
     */
    public List<Map<String, Object>> executeDistributedQuery(Query query, com.memorydb.rest.dto.QueryDto queryDto) {
        logger.info("Exécution d'une requête distribuée sur la table: {}, avec GROUP BY: {}, agrégations: {}", 
                query.getTableName(), query.getGroupByColumns(), query.getAggregateFunctions());
        logger.info("Exécution d'une requête distribuée sur la table {}", query.getTableName());
        
        // Détermine si la requête contient des GROUP BY
        boolean hasGroupBy = !query.getGroupByColumns().isEmpty();
        
        // Pour les requêtes avec GROUP BY, utilise une Map pour combiner les résultats par clé de groupe
        Map<String, Map<String, Object>> groupedResults = new HashMap<>();
        
        // Crée une liste pour les résultats agrégés de tous les nœuds (cas sans GROUP BY)
        List<Map<String, Object>> aggregatedResults = new ArrayList<>();
        
        logger.info("Démarrage de l'exécution d'une requête distribuée avec {} nœuds", getAllNodes().size());
        
        // Exécute d'abord la requête localement
        try {
            // Exécute la requête avec le QueryExecutor local
            QueryResult localResults = queryExecutor.executeQuery(query);
            logger.info("Requête exécutée avec succès sur le nœud local, {} résultats récupérés", localResults.getRowCount());
            
            if (hasGroupBy) {
                // Combine les résultats par clé de groupe
                processGroupedResults(localResults.getRows(), query, groupedResults);
            } else {
                // Ajoute simplement les résultats à la liste
                aggregatedResults.addAll(localResults.getRows());
            }
        } catch (Exception e) {
            logger.error("Erreur lors de l'exécution de la requête sur le nœud local: {}", e.getMessage(), e);
        }
        
        // Exécute la requête sur tous les autres nœuds
        for (NodeInfo node : getAllNodes()) {
            if (!node.getId().equals(localNode.getId())) {
                logger.info("Tentative d'exécution de la requête sur le nœud distant: {}", node.getId());
                try {
                    // Marque la requête comme transmise pour éviter les boucles infinies
                    queryDto.setForwardedQuery(true);
                    
                    // Utilise le client HTTP pour exécuter la requête sur le nœud distant
                    logger.info("Envoi de la requête au nœud {} ({}:{})", node.getId(), node.getAddress(), node.getPort());
                    List<Map<String, Object>> remoteResults = NodeClient.executeQuery(node, query, queryDto);
                    logger.info("Réponse reçue du nœud {}, taille des résultats: {}", node.getId(), remoteResults.size());
                    
                    if (hasGroupBy) {
                        // Combine les résultats par clé de groupe
                        processGroupedResults(remoteResults, query, groupedResults);
                    } else {
                        // Ajoute simplement les résultats à la liste
                        aggregatedResults.addAll(remoteResults);
                    }
                    
                    logger.info("Requête exécutée avec succès sur le nœud {}, {} résultats récupérés", 
                            node.getId(), remoteResults.size());
                } catch (Exception e) {
                    logger.error("Erreur lors de l'exécution de la requête sur le nœud {}: {}", 
                            node.getId(), e.getMessage(), e);
                }
            }
        }
        
        // Finalise les résultats en fonction du type de requête
        List<Map<String, Object>> finalResults;
        if (hasGroupBy) {
            // Convertit la Map des résultats groupés en liste
            finalResults = new ArrayList<>(groupedResults.values());
        } else {
            finalResults = aggregatedResults;
        }
        
        // Applique le tri global si nécessaire
        if (query.getOrderBy() != null && !query.getOrderBy().isEmpty()) {
            String orderBy = query.getOrderBy();
            boolean orderByAscending = query.isOrderByAscending();
            
            finalResults.sort((r1, r2) -> {
                Comparable val1 = (Comparable) r1.get(orderBy);
                Comparable val2 = (Comparable) r2.get(orderBy);
                
                // Gère les valeurs nulles
                if (val1 == null && val2 == null) return 0;
                if (val1 == null) return orderByAscending ? -1 : 1;
                if (val2 == null) return orderByAscending ? 1 : -1;
                
                return orderByAscending ? val1.compareTo(val2) : val2.compareTo(val1);
            });
        }
        
        // Applique la limite globale si nécessaire
        if (query.getLimit() > 0 && finalResults.size() > query.getLimit()) {
            finalResults = finalResults.subList(0, query.getLimit());
        }
        
        return finalResults;
    }
    
    /**
     * Traite les résultats d'une requête groupée et les combine avec les résultats existants
     * @param results Les résultats à traiter
     * @param query La requête d'origine
     * @param groupedResults La map des résultats groupés
     */
    private void processGroupedResults(List<Map<String, Object>> results, Query query, Map<String, Map<String, Object>> groupedResults) {
        List<String> groupByColumns = query.getGroupByColumns();
        Map<String, AggregateDefinition> aggregateFunctions = query.getAggregateFunctions();

        // Log pour le débogage
        logger.info("Traitement de {} lignes avec {} colonnes GROUP BY et {} fonctions d'agrégation", 
                results.size(), groupByColumns.size(), aggregateFunctions.size());
        logger.info("Colonnes d'agrégation: {}", aggregateFunctions.keySet());
        
        // Vérifie si les résultats sont vides
        if (results == null || results.isEmpty()) {
            logger.warn("Aucun résultat à traiter pour la requête groupée");
            return;
        }
        
        // Vérifie si les colonnes de regroupement sont définies
        if (groupByColumns == null || groupByColumns.isEmpty()) {
            logger.warn("Aucune colonne GROUP BY définie dans la requête");
            return;
        }
        
        // Pour traquer les colonnes avec préfixe et leur colonne d'origine
        Map<String, String> prefixedToOriginalColumnMap = new HashMap<>();
        for (String colName : aggregateFunctions.keySet()) {
            AggregateFunction func = aggregateFunctions.get(colName).getFunction();
            // Génère les noms de colonnes préfixés pour chaque fonction d'agrégation
            switch (func) {
                case SUM:
                    prefixedToOriginalColumnMap.put("sum_" + colName, colName);
                    break;
                case AVG:
                    prefixedToOriginalColumnMap.put("avg_" + colName, colName);
                    break;
                case MIN:
                    prefixedToOriginalColumnMap.put("min_" + colName, colName);
                    break;
                case MAX:
                    prefixedToOriginalColumnMap.put("max_" + colName, colName);
                    break;
                case COUNT:
                    // COUNT garde le nom d'origine
                    prefixedToOriginalColumnMap.put("count", colName);
                    break;
            }
        }
        logger.info("Mapping des colonnes préfixées: {}", prefixedToOriginalColumnMap);
        
        for (Map<String, Object> row : results) {
            // Vérifie que la ligne contient au moins une des colonnes GROUP BY
            boolean hasAnyGroupColumn = false;
            for (String groupCol : groupByColumns) {
                if (row.containsKey(groupCol)) {
                    hasAnyGroupColumn = true;
                    break;
                }
            }
            if (!hasAnyGroupColumn) {
                logger.warn("Ligne sans colonne GROUP BY ignorée: {}", row);
                continue;
            }
            
            // SIMPLIFIE: Utilise directement la valeur de la colonne GROUP BY comme clé
            // Cela fonctionne bien quand il n'y a qu'une seule colonne GROUP BY
            String groupKey;
            if (groupByColumns.size() == 1) {
                // Cas simple: une seule colonne de regroupement
                String groupCol = groupByColumns.get(0);
                Object value = row.get(groupCol);
                groupKey = value != null ? value.toString() : "NULL";
            } else {
                // Cas avec plusieurs colonnes de regroupement
                StringBuilder keyBuilder = new StringBuilder();
                for (String groupCol : groupByColumns) {
                    Object value = row.get(groupCol);
                    keyBuilder.append(value != null ? value.toString() : "NULL").append("|");
                }
                groupKey = keyBuilder.toString();
            }
            
            logger.info("Groupé par clé: {} pour les valeurs: {}", groupKey, 
                    groupByColumns.stream().map(col -> row.get(col)).toArray());
            
            // Si cette clé n'existe pas encore, l'ajouter
            if (!groupedResults.containsKey(groupKey)) {
                Map<String, Object> newRow = new HashMap<>();
                // Copie les valeurs des colonnes GROUP BY
                for (String groupCol : groupByColumns) {
                    newRow.put(groupCol, row.get(groupCol));
                }
                // Initialize aggregates in newRow directly from the current 'row'
                for (Map.Entry<String, AggregateDefinition> entry : aggregateFunctions.entrySet()) {
                    String aggAlias = entry.getKey(); // e.g., "count", "sum_value"
                    AggregateFunction function = entry.getValue().getFunction();
                    Object valueFromRow = null;

                    // Attempt to get the value using the alias or common names
                    // For COUNT, usually the alias is 'count'. For others, it might be 'sum_colName', 'min_colName' etc.
                    // or the direct result of QueryExecutor might already name it as 'colName' if not ambiguous.
                    if (row.containsKey(aggAlias)) {
                        valueFromRow = row.get(aggAlias);
                    } else if (function == AggregateFunction.COUNT && row.containsKey("count")) { // Common case for COUNT output from QueryExecutor
                        valueFromRow = row.get("count");
                    } else {
                        // Fallback: attempt to find by original column name for functions other than COUNT if alias is prefixed
                        // This part might need refinement based on how QueryExecutor names aggregate outputs
                        String originalColName = aggAlias;
                        if (aggAlias.contains("_")) {
                            String[] parts = aggAlias.split("_", 2);
                            if (parts.length > 1 && (parts[0].equals("sum") || parts[0].equals("avg") || parts[0].equals("min") || parts[0].equals("max"))) {
                                originalColName = parts[1];
                            }
                        }
                        if (row.containsKey(originalColName)) {
                            valueFromRow = row.get(originalColName);
                        }
                    }
                    
                    if (valueFromRow != null) {
                        // Specific handling for AVG if it's a map like {sum: S, count: C} in the row
                        // (QueryExecutor.computeAggregate for AVG returns a Map)
                        if (function == AggregateFunction.AVG && valueFromRow instanceof Map) {
                            newRow.put(aggAlias, valueFromRow); // Store the whole map {sum=S, count=C}
                        } else if (valueFromRow instanceof Number) {
                             newRow.put(aggAlias, valueFromRow); // Handles COUNT, SUM, MIN, MAX if they are numbers
                        } else {
                             // If not a number and not an AVG map, store as is or log a warning
                             logger.warn("Unexpected data type for aggregate alias {} in row {}: {}. Storing as is.", aggAlias, row, valueFromRow.getClass().getName());
                             newRow.put(aggAlias, valueFromRow); 
                        }
                    } else {
                        // If aggregate is missing from input 'row', initialize to a sensible default
                        logger.warn("Aggregate value for alias {} missing in input row {}. Initializing to default.", aggAlias, row);
                        switch (function) {
                            case COUNT:
                                newRow.put(aggAlias, 0L);
                                break;
                            case SUM:
                                newRow.put(aggAlias, 0.0D); // Assuming double for sum, adjust if Long based on column type
                                break;
                            case AVG:
                                Map<String, Object> initialAvg = new HashMap<>();
                                initialAvg.put("sum", 0.0D);
                                initialAvg.put("count", 0L);
                                newRow.put(aggAlias, initialAvg);
                                break;
                            case MIN:
                                newRow.put(aggAlias, null); // Or specific type's MAX_VALUE
                                break;
                            case MAX:
                                newRow.put(aggAlias, null); // Or specific type's MIN_VALUE
                                break;
                            default:
                                newRow.put(aggAlias, null);
                        }
                    }
                }
                groupedResults.put(groupKey, newRow);
            } else {
                // Met à jour les valeurs agrégées pour cette clé
                Map<String, Object> existingRow = groupedResults.get(groupKey);
                for (Map.Entry<String, AggregateDefinition> entry : aggregateFunctions.entrySet()) {
                    String colName = entry.getKey();
                    AggregateFunction function = entry.getValue().getFunction();
                    String resultCol;
                    
                // Modifiez la section qui traite COUNT pour afficher le contenu complet de la ligne
                if (function == AggregateFunction.COUNT) {
                    resultCol = colName;
                    
                    // Afficher le contenu complet de la ligne pour déboguer
                    logger.info("Contenu complet de la ligne pour clé {}: {}", groupKey, row);
                    
                    Object existingValObj = existingRow.get(resultCol);
                    Object incomingValObj = null;

                    // Attempt to find the incoming count value using common names/aliases.
                    // 'colName' here is the alias for the COUNT aggregate (e.g., "count").
                    String[] possibleIncomingNames = { colName, "count" }; // Prioritize the alias, then generic "count"

                    for (String name : possibleIncomingNames) {
                        if (row.containsKey(name)) {
                            incomingValObj = row.get(name);
                            logger.debug("COUNT: Found incoming value for groupKey '{}', aggCol '{}', using name '{}': {}", groupKey, colName, name, incomingValObj);
                            break;
                        }
                    }
                    if (incomingValObj == null) {
                        logger.warn("COUNT: Incoming value for groupKey '{}', aggCol '{}' not found in row keys {}. Assuming 0 for this node's contribution.", groupKey, colName, row.keySet());
                    }

                    long currentCount = 0L;
                    if (existingValObj instanceof Number) {
                        currentCount = ((Number) existingValObj).longValue();
                    } else if (existingValObj != null) {
                        // This case implies the existing stored value was not a number, which is unexpected for COUNT.
                        logger.warn("COUNT: Existing value for groupKey '{}', aggCol '{}' in existingRow is not a Number: {}. Using 0 for current count.", groupKey, colName, existingValObj);
                    } // If existingValObj is null (e.g. first merge into a default-initialized row), currentCount remains 0.

                    long countToAdd = 0L;
                    if (incomingValObj instanceof Number) {
                        countToAdd = ((Number) incomingValObj).longValue();
                    } else if (incomingValObj != null) {
                        logger.warn("COUNT: Incoming value for groupKey '{}', aggCol '{}' from current row is not a Number: {}. Adding 0.", groupKey, colName, incomingValObj);
                    } // If incomingValObj is null (already warned or logged), countToAdd remains 0.
                    
                    existingRow.put(resultCol, currentCount + countToAdd);
                    logger.info("COUNT: Merged counts for groupKey '{}', aggCol '{}': Current={}, Incoming={}, Result={}", groupKey, colName, currentCount, countToAdd, existingRow.get(resultCol));
                } else {
                        resultCol = colName;
                        // Récupère la valeur existante
                        Object existingValue = existingRow.get(resultCol);
                        
                        // Get the value to aggregate - vérifie d'abord la colonne directement
                        Object newValue = row.get(colName);
                        logger.info("Tentative d'accès à la colonne {}, valeur trouvée: {}", colName, newValue);
                        
                        // Si aucune valeur n'est trouvée avec le nom de colonne tel quel, extrait le nom de colonne réel (sans préfixe de fonction)
                        String actualColName = colName;
                        if (newValue == null && colName.contains("_")) {
                            String[] parts = colName.split("_", 2);
                            if (parts.length > 1 && (parts[0].equals("sum") || parts[0].equals("avg") || 
                                                    parts[0].equals("min") || parts[0].equals("max"))) {
                                actualColName = parts[1];
                                newValue = row.get(actualColName);
                                logger.info("Tentative d'accès avec le nom de colonne extrait {}, valeur trouvée: {}", actualColName, newValue);
                            }
                        }
                        
                        // If no value found with the original name, try with the actual column name
                        if (newValue == null && !actualColName.equals(colName)) {
                            newValue = row.get(actualColName);
                            if (newValue != null) {
                                logger.info("Found update value using extracted column name {}: {}", actualColName, newValue);
                            }
                        }
                        
                        // As a fallback, try with various prefixed names
                        if (newValue == null) {
                            String prefixedName = null;
                            switch (function) {
                                case SUM:
                                    prefixedName = "sum_" + actualColName;
                                    break;
                                case AVG:
                                    prefixedName = "avg_" + actualColName;
                                    break;
                                case MIN:
                                    prefixedName = "min_" + actualColName;
                                    break;
                                case MAX:
                                    prefixedName = "max_" + actualColName;
                                    break;
                                case COUNT:
                                    // Count typically uses the original name
                                    prefixedName = "count";
                                    break;
                            }
                            
                            if (prefixedName != null && row.containsKey(prefixedName)) {
                                newValue = row.get(prefixedName);
                                logger.info("Utilisation de la valeur préfixée pour mise à jour: {} = {}", prefixedName, newValue);
                            }
                        }
                        
                        // Skip null values for calculations
                        if (newValue == null) {
                            continue;
                        }
                        
                        // Mise à jour selon la fonction d'agrégation
                        switch (function) {
                            case COUNT:
                                // Déjà traité en dehors du switch
                                break;
                            case SUM:
                                if (existingValue instanceof Number && newValue instanceof Number) {
                                    Number sum;
                                    if (existingValue instanceof Double || newValue instanceof Double) {
                                        sum = ((Number) existingValue).doubleValue() + ((Number) newValue).doubleValue();
                                    } else if (existingValue instanceof Float || newValue instanceof Float) {
                                        sum = ((Number) existingValue).floatValue() + ((Number) newValue).floatValue();
                                    } else if (existingValue instanceof Long || newValue instanceof Long) {
                                        sum = ((Number) existingValue).longValue() + ((Number) newValue).longValue();
                                    } else { // Integer, Short, Byte default to int sum
                                        sum = ((Number) existingValue).intValue() + ((Number) newValue).intValue();
                                    }
                                    existingRow.put(resultCol, sum);
                                } else if (newValue instanceof Number && existingValue == null) { // If existing is null (e.g. first sum for a group after init)
                                     existingRow.put(resultCol, newValue);
                                } else if (newValue instanceof Number) { // If existing is not a number but new is, prefer new.
                                    logger.warn("SUM merge: existingValue for groupKey {} was not a number. Overwriting with newValue. existing: {}, new: {}", groupKey, existingValue, newValue);
                                    existingRow.put(resultCol, newValue);
                                } else {
                                    logger.warn("SUM merge: Values are not suitable for summation for groupKey {}. existing: {}, new: {}. Skipping update.", groupKey, existingValue, newValue);
                                }
                                break;
                            case AVG:
                                if (existingValue instanceof Map && newValue instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> existingAvgComponents = (Map<String, Object>) existingValue;
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> incomingAvgComponents = (Map<String, Object>) newValue;

                                    Object existingSumObj = existingAvgComponents.get("sum");
                                    Object existingCountObj = existingAvgComponents.get("count");
                                    Object incomingSumObj = incomingAvgComponents.get("sum");
                                    Object incomingCountObj = incomingAvgComponents.get("count");

                                    if (existingSumObj instanceof Number && existingCountObj instanceof Number &&
                                        incomingSumObj instanceof Number && incomingCountObj instanceof Number) {

                                        double totalSum = ((Number) existingSumObj).doubleValue() + ((Number) incomingSumObj).doubleValue();
                                        long totalCount = ((Number) existingCountObj).longValue() + ((Number) incomingCountObj).longValue();

                                        Map<String, Object> newAvgComponents = new HashMap<>();
                                        newAvgComponents.put("sum", totalSum);
                                        newAvgComponents.put("count", totalCount);
                                        existingRow.put(resultCol, newAvgComponents); // resultCol is aggAlias
                                    } else {
                                        logger.warn("AVG merge: One or more sum/count components are not numbers for groupKey {}. existing: {}, incoming: {}. Skipping update.", groupKey, existingAvgComponents, incomingAvgComponents);
                                    }
                                } else {
                                     logger.warn("AVG merge: existingValue or newValue is not a Map for groupKey {}. existing type: {}, new type: {}. Skipping update.",
                                        groupKey,
                                        existingValue != null ? existingValue.getClass().getName() : "null",
                                        newValue != null ? newValue.getClass().getName() : "null");
                                }
                                break;
                            case MIN:
                                // Conserve la valeur minimale
                                if (existingValue instanceof Comparable && newValue instanceof Comparable) {
                                    @SuppressWarnings("unchecked")
                                    Comparable<Object> compExisting = (Comparable<Object>) existingValue;
                                    if (compExisting.compareTo(newValue) > 0) {
                                        existingRow.put(resultCol, newValue);
                                    }
                                }
                                break;
                            case MAX:
                                // Conserve la valeur maximale
                                if (existingValue instanceof Comparable && newValue instanceof Comparable) {
                                    @SuppressWarnings("unchecked")
                                    Comparable<Object> compExisting = (Comparable<Object>) existingValue;
                                    if (compExisting.compareTo(newValue) < 0) {
                                        existingRow.put(resultCol, newValue);
                                    }
                                }
                                break;
                        }
                    }
                }
            }
        }
        
        // Log le nombre de groupes trouvés
        logger.info("Nombre total de groupes trouvés: {}", groupedResults.size());
        
        // Finalise les calculs et nettoie les champs temporaires
        for (Map<String, Object> row : groupedResults.values()) {
            for (Map.Entry<String, AggregateDefinition> entry : aggregateFunctions.entrySet()) {
                String colName = entry.getKey();
                AggregateFunction function = entry.getValue().getFunction();
                
                if (function == AggregateFunction.AVG) {
                    Object sum = row.get(colName);
                    Object count = row.get("__count_" + colName);
                    if (sum instanceof Number && count instanceof Number) {
                        double avg = ((Number) sum).doubleValue() / ((Number) count).doubleValue();
                        row.put(colName, avg);
                    }
                    // Retire le compteur temporaire
                    row.remove("__count_" + colName);
                }
                
                // Traite les valeurs spéciales pour MIN et MAX
                if (function == AggregateFunction.MIN && row.get(colName) instanceof Double) {
                    double val = (Double) row.get(colName);
                    if (val == Double.MAX_VALUE) {
                        row.put(colName, 0.0); // Remplace par une valeur par défaut plus raisonnable
                    }
                }
                
                if (function == AggregateFunction.MAX && row.get(colName) instanceof Double) {
                    double val = (Double) row.get(colName);
                    if (val == Double.MIN_VALUE) {
                        row.put(colName, 0.0); // Remplace par une valeur par défaut plus raisonnable
                    }
                }
            }
            
            // Supprime tous les champs temporaires commençant par __
            row.entrySet().removeIf(entry -> entry.getKey().startsWith("__"));
        }
    }
    
    /**
     * Synchronise toutes les tables existantes avec un nœud spécifique
     * @param node Le nœud avec lequel synchroniser
     */
    private void synchronizeTablesWithNode(NodeInfo node) {
        logger.info("Synchronisation des tables existantes avec le nœud {}", node.getId());
        
        List<Table> tables = databaseContext.getAllTables();
        for (Table table : tables) {
            try {
                boolean success = NodeClient.sendTableCreationRequest(node, table.getName(), table.getColumns());
                if (success) {
                    logger.info("Table '{}' synchronisée avec succès avec le nœud {}", table.getName(), node.getId());
                } else {
                    logger.error("Échec de la synchronisation de la table '{}' avec le nœud {}", table.getName(), node.getId());
                }
            } catch (Exception e) {
                logger.error("Erreur lors de la synchronisation de la table '{}' avec le nœud {}: {}", 
                        table.getName(), node.getId(), e.getMessage());
            }
        }
    }
    
    /**
     * Rééquilibre les données entre les nœuds du cluster
     */
    private void rebalanceData() {
        // Cette méthode peut être implémentée pour rééquilibrer les données entre les nœuds
        // Par exemple, en cas de suppression d'un nœud, les données peuvent être redistribuées
        // Cette implémentation basique ne fait rien pour le moment
        logger.info("Rééquilibrage des données non implémenté");
    }
    
    /**
     * Récupère les statistiques d'une table depuis tous les nœuds du cluster
     * Cette méthode interroge chaque nœud pour obtenir ses statistiques locales
     * 
     * @param tableName Le nom de la table
     * @return Une map contenant les statistiques de chaque nœud (clé = id du nœud, valeur = statistiques)
     */
    public Map<String, Map<String, Object>> getAllNodesStats(String tableName) {
        logger.info("Récupération des statistiques pour la table '{}' sur tous les nœuds", tableName);
        Map<String, Map<String, Object>> allStats = new HashMap<>();
        
        // Obtenir les statistiques du nœud local d'abord
        try {
            if (databaseContext.tableExists(tableName)) {
                Map<String, Object> localStats = getLocalTableStats(tableName);
                allStats.put(localNode.getId(), localStats);
                logger.debug("Statistiques locales récupérées pour la table '{}'", tableName);
            }
        } catch (Exception e) {
            logger.error("Erreur lors de la récupération des statistiques locales pour '{}': {}", 
                    tableName, e.getMessage(), e);
        }
        
        // Pour chaque nœud distant, récupérer ses statistiques en parallèle
        List<NodeInfo> remoteNodes = clusterNodes.values().stream()
                .filter(node -> !node.getId().equals(localNode.getId()))
                .toList();
        
        // Créer les tâches pour récupérer les statistiques en parallèle
        List<Map.Entry<String, Map<String, Object>>> remoteResults = Collections.synchronizedList(new ArrayList<>());
        
        // Lancer toutes les requêtes en parallèle
        for (NodeInfo node : remoteNodes) {
            executorService.submit(() -> {
                try {
                    Map<String, Object> nodeStats = NodeClient.getTableStats(node, tableName);
                    if (nodeStats != null) {
                        remoteResults.add(Map.entry(node.getId(), nodeStats));
                        logger.debug("Statistiques récupérées pour la table '{}' sur le nœud {}", 
                                tableName, node.getId());
                    }
                } catch (Exception e) {
                    logger.error("Erreur lors de la récupération des statistiques pour '{}' sur le nœud {}: {}", 
                            tableName, node.getId(), e.getMessage(), e);
                }
            });
        }
        
        // Attendre que toutes les requêtes soient terminées
        try {
            // Attente courte pour laisser le temps aux requêtes de se terminer
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Ajouter tous les résultats à la map principale
        for (Map.Entry<String, Map<String, Object>> entry : remoteResults) {
            allStats.put(entry.getKey(), entry.getValue());
        }
        
        logger.info("Statistiques récupérées pour {} nœuds sur la table '{}'", allStats.size(), tableName);
        return allStats;
    }
    
    /**
     * Récupère les statistiques locales d'une table
     * 
     * @param tableName Le nom de la table
     * @return Les statistiques de la table sur ce nœud
     */
    private Map<String, Object> getLocalTableStats(String tableName) {
        // Vérifier si la table existe
        if (!databaseContext.tableExists(tableName)) {
            return null;
        }
        
        // Utiliser l'API locale pour récupérer les statistiques
        // Cela évite de dupliquer le code de calcul des statistiques
        // et assure la cohérence entre les différentes parties de l'application
        NodeInfo localNode = getLocalNode();
        try {
            return NodeClient.getTableStats(localNode, tableName);
        } catch (Exception e) {
            logger.error("Erreur lors de la récupération des statistiques locales pour '{}': {}", 
                    tableName, e.getMessage(), e);
            return null;
        }
    }
} 