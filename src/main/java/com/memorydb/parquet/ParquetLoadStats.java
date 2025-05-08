package com.memorydb.parquet;

/**
 * Statistiques de chargement d'un fichier Parquet
 */
public class ParquetLoadStats {
    private long rowsProcessed;
    private int batchCount;
    private long elapsedTimeMs;
    private boolean timeout;
    private String error;
    
    /**
     * Constructeur par défaut
     */
    public ParquetLoadStats() {
        this.rowsProcessed = 0;
        this.batchCount = 0;
        this.elapsedTimeMs = 0;
        this.timeout = false;
        this.error = null;
    }
    
    /**
     * Obtient le nombre de lignes traitées
     * @return Nombre de lignes
     */
    public long getRowsProcessed() {
        return rowsProcessed;
    }
    
    /**
     * Définit le nombre de lignes traitées
     * @param rowsProcessed Nombre de lignes
     */
    public void setRowsProcessed(long rowsProcessed) {
        this.rowsProcessed = rowsProcessed;
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
        return (rowsProcessed * 1000.0) / elapsedTimeMs;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Lignes traitées: ").append(rowsProcessed);
        sb.append(", Batchs: ").append(batchCount);
        sb.append(", Temps: ").append(elapsedTimeMs / 1000.0).append("s");
        sb.append(", Vitesse: ").append(String.format("%.2f", getRowsPerSecond())).append(" lignes/s");
        
        if (timeout) {
            sb.append(" (Timeout)");
        }
        
        if (error != null) {
            sb.append(" (Erreur: ").append(error).append(")");
        }
        
        return sb.toString();
    }
}
