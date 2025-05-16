package com.memorydb.parquet;

import java.util.Map;
import java.util.HashMap;

/**
 * Classe pour les statistiques de chargement de fichiers Parquet
 * Supporté également le suivi des lignes par nœud pour la distribution
 */
public class ParquetLoadStats {
    private long totalRowsProcessed;
    private Map<String, Long> nodeRowsMap;
    private int batchCount;
    private long elapsedTimeMs;
    private boolean timeout;
    private String error;
    
    /**
     * Constructeur par défaut
     */
    public ParquetLoadStats() {
        this.totalRowsProcessed = 0;
        this.nodeRowsMap = new HashMap<>();
        this.batchCount = 0;
        this.elapsedTimeMs = 0;
        this.timeout = false;
        this.error = null;
    }
    
    /**
     * Obtient le nombre total de lignes traitées
     * @return Nombre total de lignes
     */
    public long getRowsProcessed() {
        return totalRowsProcessed;
    }
    
    /**
     * Définit le nombre total de lignes traitées
     * @param rowsProcessed Nombre total de lignes
     */
    public void setRowsProcessed(long rowsProcessed) {
        this.totalRowsProcessed = rowsProcessed;
    }
    
    /**
     * Incrémente le nombre total de lignes traitées
     * @param count Nombre de lignes à ajouter
     */
    public void incrementRowsProcessed(long count) {
        this.totalRowsProcessed += count;
    }
    
    /**
     * Ajoute des lignes à un nœud spécifique
     * @param nodeId Identifiant du nœud
     * @param count Nombre de lignes
     */
    public void addNodeRows(String nodeId, long count) {
        nodeRowsMap.put(nodeId, nodeRowsMap.getOrDefault(nodeId, 0L) + count);
        incrementRowsProcessed(count);
    }
    
    /**
     * Obtient le nombre de lignes pour un nœud spécifique
     * @param nodeId Identifiant du nœud
     * @return Nombre de lignes attribuées au nœud, 0 si aucune
     */
    public long getNodeRows(String nodeId) {
        return nodeRowsMap.getOrDefault(nodeId, 0L);
    }
    
    /**
     * Obtient la carte complète des lignes par nœud
     * @return Map des lignes par nœud
     */
    public Map<String, Long> getNodeRowsMap() {
        return new HashMap<>(nodeRowsMap);
    }
    
    /**
     * Obtient le nombre de batchs traités
     * @return Nombre de batchs
     */
    public int getBatchCount() {
        return batchCount;
    }
    
    /**
     * Définit le nombre de batchs traités
     * @param batchCount Nombre de batchs
     */
    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }
    
    /**
     * Obtient le temps d'exécution en millisecondes
     * @return Temps d'exécution
     */
    public long getElapsedTimeMs() {
        return elapsedTimeMs;
    }
    
    /**
     * Définit le temps d'exécution en millisecondes
     * @param elapsedTimeMs Temps d'exécution
     */
    public void setElapsedTimeMs(long elapsedTimeMs) {
        this.elapsedTimeMs = elapsedTimeMs;
    }
    
    /**
     * Vérifie si le chargement a été interrompu par un timeout
     * @return true si timeout
     */
    public boolean isTimeout() {
        return timeout;
    }
    
    /**
     * Définit si le chargement a été interrompu par un timeout
     * @param timeout true si timeout
     */
    public void setTimeout(boolean timeout) {
        this.timeout = timeout;
    }
    
    /**
     * Obtient le message d'erreur éventuel
     * @return Message d'erreur ou null
     */
    public String getError() {
        return error;
    }
    
    /**
     * Définit le message d'erreur
     * @param error Message d'erreur
     */
    public void setError(String error) {
        this.error = error;
    }
    
    /**
     * Vérifie si le chargement a réussi (pas d'erreur et pas de timeout)
     * @return true si succès
     */
    public boolean isSuccess() {
        return error == null && !timeout;
    }
    
    /**
     * Calcule la vitesse de chargement en lignes par seconde
     * @return Lignes par seconde
     */
    public double getRowsPerSecond() {
        if (elapsedTimeMs <= 0) return 0;
        return (totalRowsProcessed * 1000.0) / elapsedTimeMs;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Lignes traitées: ").append(totalRowsProcessed);
        sb.append(", Batchs: ").append(batchCount);
        sb.append(", Temps: ").append(elapsedTimeMs / 1000.0).append("s");
        sb.append(", Vitesse: ").append(String.format("%.2f", getRowsPerSecond())).append(" lignes/s");
        
        // Ajouter détails de distribution par nœud si disponible
        if (!nodeRowsMap.isEmpty()) {
            sb.append("\nDistribution par nœud: ");
            for (Map.Entry<String, Long> entry : nodeRowsMap.entrySet()) {
                sb.append("\n  ").append(entry.getKey()).append(": ").append(entry.getValue());
            }
        }
        
        if (timeout) {
            sb.append(" (Timeout)");
        }
        
        if (error != null) {
            sb.append(" (Erreur: ").append(error).append(")");
        }
        
        return sb.toString();
    }
}
