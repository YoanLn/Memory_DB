package com.memorydb.distribution;

import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.query.AggregateFunction;
import com.memorydb.query.Query;
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
        Map<String, AggregateFunction> aggregateFunctions = query.getAggregateFunctions();
        
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
            AggregateFunction func = aggregateFunctions.get(colName);
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
                
                // Initialise les colonnes d'agrégation
                for (Map.Entry<String, AggregateFunction> entry : aggregateFunctions.entrySet()) {
                    String colName = entry.getKey();
                    AggregateFunction function = entry.getValue();
                    String resultCol;
                    
                    // Détermine la colonne résultat selon la fonction
                    if (function == AggregateFunction.COUNT) {
                        resultCol = colName; // Pour COUNT, utilise directement le nom spécifié
                        newRow.put(resultCol, 1L); // Démarre le compteur à 1
                    } else {
                        // Extract the actual column name without any prefix
                        String actualColName = colName;
                        if (colName.contains("_")) {
                            String[] parts = colName.split("_", 2);
                            if (parts.length > 1 && (parts[0].equals("sum") || parts[0].equals("avg") || 
                                                    parts[0].equals("min") || parts[0].equals("max"))) {
                                actualColName = parts[1];
                                logger.info("Extracted actual column name: {} from prefixed name: {}", actualColName, colName);
                            }
                        }
                        
                        // First try to get the value directly
                        Object value = row.get(colName);
                        
                        // If no value found with the prefixed name, try with the actual column name
                        if (value == null && !actualColName.equals(colName)) {
                            value = row.get(actualColName);
                            if (value != null) {
                                logger.info("Found value using extracted column name {}: {}", actualColName, value);
                            }
                        }
                        
                        // Essaie avec différents noms préfixés comme solution de repli
                        if (value == null) {
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
                                    // Pour COUNT on garde le nom d'origine
                                    prefixedName = "count";
                                    break;
                            }
                            
                            if (prefixedName != null) {
                                if (row.containsKey(prefixedName)) {
                                    value = row.get(prefixedName);
                                    logger.info("Utilisation de la valeur préfixée: {} = {}", prefixedName, value);
                                } else {
                                    // Tenter une recherche insensible à la casse
                                    for (String key : row.keySet()) {
                                        if (key.equalsIgnoreCase(prefixedName) || 
                                            key.equalsIgnoreCase(actualColName)) {
                                            value = row.get(key);
                                            logger.info("Trouvé valeur avec correspondance insensible à la casse: {} -> {}", key, value);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Si toujours pas de valeur, recherche dans toutes les clés disponibles
                        if (value == null) {
                            // Recherche dans toutes les clés de la ligne
                            for (String key : row.keySet()) {
                                if (key.toLowerCase().contains(actualColName.toLowerCase())) {
                                    value = row.get(key);
                                    logger.info("Trouvé valeur en utilisant une correspondance partielle: {}={}", key, value);
                                    break;
                                }
                            }
                            
                            // Si encore null, on fait une dernière tentative avec la colonne brute
                            if (value == null) {
                                value = row.get(actualColName);
                                if (value != null) {
                                    logger.info("Trouvé valeur en utilisant le nom de colonne brut: {}", value);
                                }
                            }
                            
                            // Si toujours null, utilise une valeur par défaut
                            if (value == null) {
                                // Initialise avec des valeurs par défaut appropriées si la colonne est absente
                                switch (function) {
                                    case COUNT:
                                        value = 1L; // Pour COUNT on commence à 1 pour la première ligne
                                        break;
                                    case SUM:
                                        value = 0.0;
                                        break;
                                    case AVG:
                                        value = 0.0;
                                        break;
                                    case MIN:
                                        value = Double.MAX_VALUE; // Valeur élevée pour MIN
                                        break;
                                    case MAX:
                                        value = Double.MIN_VALUE; // Valeur basse pour MAX
                                        break;
                                }
                                logger.info("Valeur par défaut initialisée pour {} avec la fonction {}: {}", colName, function, value);
                            }
                        }
                        
                        // Stocke la valeur initiale
                        resultCol = colName;
                        newRow.put(resultCol, value);
                        
                        // Pour AVG, stocke aussi le compteur
                        if (function == AggregateFunction.AVG) {
                            newRow.put("__count_" + colName, 1L);
                        }
                    }
                }
                groupedResults.put(groupKey, newRow);
            } else {
                // Met à jour les valeurs agrégées pour cette clé
                Map<String, Object> existingRow = groupedResults.get(groupKey);
                for (Map.Entry<String, AggregateFunction> entry : aggregateFunctions.entrySet()) {
                    String colName = entry.getKey();
                    AggregateFunction function = entry.getValue();
                    String resultCol;
                    
                    // Détermine la colonne résultat selon la fonction
                    if (function == AggregateFunction.COUNT) {
                        resultCol = colName;
                        // Incrémente le compteur
                        Object existingValue = existingRow.get(resultCol);
                        if (existingValue instanceof Number) {
                            existingRow.put(resultCol, ((Number) existingValue).longValue() + 1);
                        }
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
                                // Additionne les valeurs
                                if (existingValue instanceof Number && newValue instanceof Number) {
                                    double sum = ((Number) existingValue).doubleValue() + ((Number) newValue).doubleValue();
                                    existingRow.put(resultCol, sum);
                                }
                                break;
                            case AVG:
                                // Accumule la somme et le compteur pour calculer la moyenne à la fin
                                if (existingValue instanceof Number && newValue instanceof Number) {
                                    double sum = ((Number) existingValue).doubleValue() + ((Number) newValue).doubleValue();
                                    long count = ((Number) existingRow.get("__count_" + colName)).longValue() + 1;
                                    existingRow.put(resultCol, sum);
                                    existingRow.put("__count_" + colName, count);
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
            for (Map.Entry<String, AggregateFunction> entry : aggregateFunctions.entrySet()) {
                String colName = entry.getKey();
                AggregateFunction function = entry.getValue();
                
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
} 