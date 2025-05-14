package com.memorydb.distribution;

import com.memorydb.parquet.ParquetLoadOptions;
import com.memorydb.parquet.ParquetLoadStats;
import com.memorydb.parquet.VectorizedParquetLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Gestionnaire pour le chargement distribué des fichiers Parquet
 * Répartit les données en round-robin entre les nœuds du cluster
 */
@Singleton
public class DistributedParquetLoader {

    private static final Logger logger = LoggerFactory.getLogger(DistributedParquetLoader.class);
    
    // Cache des fichiers distribués pour éviter les redistributions inutiles
    private static final Map<String, Map<String, Boolean>> distributedFilesCache = new ConcurrentHashMap<>();
    
    @Inject
    private ClusterManager clusterManager;
    
    @Inject
    private VectorizedParquetLoader parquetLoader;
    
    /**
     * Charge un fichier Parquet de manière distribuée, en répartissant les lignes entre les nœuds du cluster
     * selon une stratégie round-robin
     * 
     * @param tableName Le nom de la table où charger les données
     * @param filePath Le chemin du fichier Parquet
     * @param options Les options de chargement
     * @return Une map contenant le nombre de lignes chargées par nœud
     */
    public Map<String, Long> loadDistributed(String tableName, String filePath, ParquetLoadOptions options) throws IOException {
        // Obtient les nœuds du cluster
        Collection<NodeInfo> nodes = clusterManager.getAllNodes();
        Map<String, Long> resultMap = new HashMap<>();
        
        // Initialise les résultats à 0 pour tous les nœuds
        for (NodeInfo node : nodes) {
            resultMap.put(node.getId(), 0L);
        }
        
        // Vérification et propagation du fichier à tous les nœuds avant de commencer le traitement
        ensureFileAvailableOnAllNodes(filePath, nodes);
        
        // Si un seul nœud, pas besoin de distribution
        if (nodes.size() <= 1) {
            logger.info("Mode distribué ignoré - un seul nœud disponible");
            ParquetLoadStats stats = parquetLoader.loadParquetFile(tableName, filePath, options);
            resultMap.put(clusterManager.getLocalNode().getId(), stats.getRowsProcessed());
            return resultMap;
        }
        
        // Détermine le nombre total de lignes à charger
        long totalRows;
        
        // OPTIMISATION AVANCÉE: Pour un petit nombre de lignes, évite complètement de compter
        if (options.getRowLimit() > 0) {
            // Utilise directement la limite comme nombre total de lignes
            totalRows = options.getRowLimit();
            logger.info("Optimisation: utilisation directe de la limite de {} lignes sans comptage complet", totalRows);
        } else {
            // Pour les chargements sans limite, compte l'ensemble du fichier
            long startTime = System.currentTimeMillis();
            ParquetLoadStats countStats = parquetLoader.countParquetRows(filePath);
            long countDuration = System.currentTimeMillis() - startTime;
            
            totalRows = countStats.getRowsProcessed();
            logger.info("Nombre total de lignes dans le fichier: {} (comptage en {} ms)", totalRows, countDuration);
        }
        
        // Si aucune ligne, rien à faire
        if (totalRows <= 0) {
            logger.info("Aucune ligne à charger");
            return resultMap;
        }
        
        // Convertit les nœuds en array pour itération indexée
        NodeInfo[] nodeArray = nodes.toArray(new NodeInfo[0]);
        int nodeCount = nodeArray.length;
        
        logger.info("Distribution des {} lignes en round-robin entre {} nœuds", totalRows, nodeCount);
        
        // APPROCHE OPTIMALE: Utilisation du traitement par colonnes natif de Parquet
        logger.info("Distribution optimisée en mode column-oriented pour {} lignes", totalRows);
        
        // Calcule le nombre de lignes par nœud (distribution équitable)
        long rowsPerNode = totalRows / nodeCount;
        long remainingRows = totalRows % nodeCount;
        
        // ID du nœud local
        String localNodeId = clusterManager.getLocalNode().getId();
        
        // Détermine les plages de lignes pour chaque nœud
        for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
            NodeInfo node = nodeArray[nodeIndex];
            long startRow = nodeIndex * rowsPerNode + Math.min(nodeIndex, remainingRows);
            long nodeRowCount = rowsPerNode + (nodeIndex < remainingRows ? 1 : 0);
            long endRow = startRow + nodeRowCount - 1;
            
            logger.info("Nœud {} se voit attribuer les lignes {} à {}", 
                    node.getId(), startRow, endRow);
            
            if (node.getId().equals(localNodeId)) {
                // Charge localement un lot de lignes en mode colonne
                logger.info("Chargement local en mode colonne: {} lignes à partir de {}", nodeRowCount, startRow);
                
                ParquetLoadOptions localOptions = new ParquetLoadOptions();
                localOptions.setBatchSize(options.getBatchSize());
                localOptions.setSkipRows((int)startRow);
                localOptions.setRowLimit((int)nodeRowCount);
                localOptions.setUseDirectAccess(true);  // Utilise l'accès direct aux colonnes
                
                try {
                    long startTime = System.currentTimeMillis();
                    ParquetLoadStats stats = parquetLoader.loadParquetFile(tableName, filePath, localOptions);
                    long duration = System.currentTimeMillis() - startTime;
                    
                    resultMap.put(localNodeId, stats.getRowsProcessed());
                    logger.info("Chargement local terminé: {} lignes en {} ms ({} lignes/sec)", 
                            stats.getRowsProcessed(), duration, stats.getRowsProcessed() * 1000 / Math.max(1, duration));
                } catch (Exception e) {
                    logger.error("Erreur lors du chargement local du lot: {}", e.getMessage());
                }
            } else {
                // Envoie une requête de chargement par lot en mode colonne au nœud distant
                try {
                    logger.info("Envoi de la demande de chargement par lot en mode colonne au nœud {}", node.getId());
                    
                    // Utilise l'API de chargement par lot plutôt que le traitement ligne par ligne
                    boolean success = sendLoadRangeRequest(
                            node, tableName, filePath, 
                            (int)startRow, (int)nodeRowCount, 
                            options.getBatchSize());
                    
                    if (success) {
                        resultMap.put(node.getId(), nodeRowCount);
                        logger.info("Lot envoyé avec succès au nœud {} ({} lignes)", node.getId(), nodeRowCount);
                    } else {
                        logger.error("Échec de l'envoi du lot au nœud {}", node.getId());
                    }
                } catch (Exception e) {
                    logger.error("Erreur lors de l'envoi du lot au nœud {}: {}", 
                            node.getId(), e.getMessage());
                }
            }
        }
        
        // Calcule et affiche les statistiques
        long totalProcessed = resultMap.values().stream().mapToLong(Long::longValue).sum();
        logger.info("Distribution terminée, total des lignes chargées: {}/{}", totalProcessed, totalRows);
        
        return resultMap;
    }
    
    /**
     * Envoie une demande de chargement d'une ligne spécifique à un nœud distant
     * 
     * @param node Le nœud distant
     * @param tableName Le nom de la table
     * @param filePath Chemin du fichier Parquet
     * @param rowIndex Indice de la ligne à charger
     * @return true si l'opération a réussi
     * @deprecated Cette méthode est désormais obsolète, utilisez plutôt le chargement par lot avec sendLoadRangeRequest
     */
    @Deprecated
    private boolean sendLoadSpecificRowRequest(NodeInfo node, String tableName, String filePath, int rowIndex) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("filePath", filePath);
        payload.put("rowIndex", rowIndex);
        
        return NodeClient.sendLoadSpecificRowRequest(node, tableName, payload);
    }
    
    /**
     * Envoie une demande de chargement d'une plage de lignes à un nœud distant
     * Cette méthode est utilisée pour l'optimisation de chargement des grands ensembles de données
     * 
     * @param node Le nœud distant
     * @param tableName Le nom de la table
     * @param filePath Chemin du fichier Parquet
     * @param startRow Index de la première ligne à charger
     * @param rowCount Nombre de lignes à charger
     * @param batchSize Taille des lots pour le chargement
     * @return true si l'opération a réussi
     */
    private boolean sendLoadRangeRequest(NodeInfo node, String tableName, String filePath, 
                                         int startRow, int rowCount, int batchSize) {
        return NodeClient.sendLoadRangeRequest(node, tableName, filePath, startRow, rowCount, batchSize);
    }
    
    /**
     * Vérifie que tous les nœuds ont accès au fichier et le propage si nécessaire
     * Cette méthode assure que le fichier est disponible sur tous les nœuds avant le traitement distribué
     * 
     * @param filePath Chemin du fichier Parquet à distribuer
     * @param nodes Liste des nœuds du cluster
     * @return true si tous les nœuds ont accès au fichier, false sinon
     */
    private boolean ensureFileAvailableOnAllNodes(String filePath, Collection<NodeInfo> nodes) {
        if (filePath == null || filePath.isEmpty() || nodes.isEmpty()) {
            return false;
        }
        
        // ID du nœud local
        String localNodeId = clusterManager.getLocalNode().getId();
        File localFile = new File(filePath);
        
        if (!localFile.exists()) {
            logger.error("Le fichier local n'existe pas: {}", filePath);
            return false;
        }
        
        // Normalise le chemin du fichier (nom du fichier sans répertoire)
        String filename = localFile.getName();
        logger.info("Vérification de la disponibilité du fichier {} sur tous les nœuds", filename);
        
        // Récupère ou initialise le cache pour ce fichier
        Map<String, Boolean> nodeAvailabilityMap = distributedFilesCache.computeIfAbsent(
                filename, k -> new ConcurrentHashMap<>());
        
        // Compte le nombre de nœuds qui ont besoin du fichier
        AtomicBoolean allNodesHaveFile = new AtomicBoolean(true);
        int nodesToSyncCount = 0;
        
        for (NodeInfo node : nodes) {
            // Ignore le nœud local qui a déjà le fichier
            if (node.getId().equals(localNodeId)) {
                nodeAvailabilityMap.put(localNodeId, true);
                continue;
            }
            
            // Vérifie si le nœud a déjà le fichier selon notre cache
            Boolean nodeHasFile = nodeAvailabilityMap.get(node.getId());
            
            // Si non présent dans le cache, vérification via API
            if (nodeHasFile == null) {
                nodeHasFile = NodeClient.checkFileExists(node, filePath);
                nodeAvailabilityMap.put(node.getId(), nodeHasFile);
            }
            
            if (!nodeHasFile) {
                allNodesHaveFile.set(false);
                nodesToSyncCount++;
            }
        }
        
        // Si tous les nœuds ont déjà le fichier, rien à faire
        if (allNodesHaveFile.get()) {
            logger.info("Tous les nœuds ont déjà accès au fichier {}", filename);
            return true;
        }
        
        logger.info("{} nœuds nécessitent une synchronisation du fichier {}", nodesToSyncCount, filename);
        
        // Propagation du fichier aux nœuds qui ne l'ont pas
        boolean success = true;
        for (NodeInfo node : nodes) {
            // Ignore le nœud local
            if (node.getId().equals(localNodeId)) {
                continue;
            }
            
            Boolean nodeHasFile = nodeAvailabilityMap.get(node.getId());
            
            if (nodeHasFile == null || !nodeHasFile) {
                logger.info("Envoi du fichier {} au nœud {}", filename, node.getId());
                
                // Envoie le fichier au nœud
                String remotePath = NodeClient.sendFile(node, filePath);
                
                if (remotePath != null) {
                    nodeAvailabilityMap.put(node.getId(), true);
                    logger.info("Fichier {} envoyé avec succès au nœud {}, chemin distant: {}", 
                            filename, node.getId(), remotePath);
                } else {
                    success = false;
                    logger.error("Échec de l'envoi du fichier {} au nœud {}", filename, node.getId());
                }
            }
        }
        
        // Mise à jour du cache global
        distributedFilesCache.put(filename, nodeAvailabilityMap);
        
        if (success) {
            logger.info("Synchronisation du fichier {} terminée avec succès sur tous les nœuds", filename);
        } else {
            logger.warn("Synchronisation du fichier {} incomplète, certains nœuds n'ont pas le fichier", filename);
        }
        
        return success;
    }
}
