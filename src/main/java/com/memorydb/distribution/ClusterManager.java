package com.memorydb.distribution;

import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.query.Query;
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
     * @return Les résultats agrégés
     */
    public List<Map<String, Object>> executeDistributedQuery(Query query) {
        logger.info("Exécution d'une requête distribuée sur la table '{}'", query.getTableName());
        
        List<Map<String, Object>> localResults = databaseContext.executeQuery(query);
        List<Map<String, Object>> aggregatedResults = new ArrayList<>(localResults);
        
        // Pour chaque nœud (sauf le nœud local), exécute la requête
        for (NodeInfo node : getAllNodes()) {
            if (!node.getId().equals(localNode.getId())) {
                try {
                    // Utilise le client HTTP pour exécuter la requête
                    List<Map<String, Object>> remoteResults = NodeClient.executeQuery(node, query);
                    aggregatedResults.addAll(remoteResults);
                    logger.info("Requête exécutée avec succès sur le nœud {}, {} résultats récupérés", 
                            node.getId(), remoteResults.size());
                } catch (Exception e) {
                    logger.error("Erreur lors de l'exécution de la requête sur le nœud {}: {}", 
                            node.getId(), e.getMessage());
                }
            }
        }
        
        // Applique le tri global si nécessaire
        if (query.getOrderBy() != null && !query.getOrderBy().isEmpty()) {
            String orderBy = query.getOrderBy();
            boolean orderByAscending = query.isOrderByAscending();
            
            aggregatedResults.sort((r1, r2) -> {
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
        if (query.getLimit() > 0 && aggregatedResults.size() > query.getLimit()) {
            aggregatedResults = aggregatedResults.subList(0, query.getLimit());
        }
        
        return aggregatedResults;
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