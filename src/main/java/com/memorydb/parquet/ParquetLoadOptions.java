package com.memorydb.parquet;

import java.util.Map;

/**
 * Options avancées pour le chargement de fichiers Parquet
 */
public class ParquetLoadOptions {
    private long rowLimit = -1; // Pas de limite par défaut
    private int batchSize = 10000; // Taille de batch par défaut
    private int timeoutSeconds = 0; // Pas de timeout par défaut
    private boolean useDirectAccess = true; // Utiliser l'accès direct aux colonnes
    private int parallelism = Runtime.getRuntime().availableProcessors(); // Parallélisme par défaut
    private Map<String, Object> filterOptions = null; // Options de filtrage (pour la distribution)
    private int skipRows = 0; // Nombre de lignes à sauter au début
    
    /**
     * Constructeur par défaut
     */
    public ParquetLoadOptions() {
    }
    
    /**
     * Constructeur avec limite de lignes
     * @param rowLimit Nombre maximum de lignes à charger
     */
    public ParquetLoadOptions(long rowLimit) {
        this.rowLimit = rowLimit;
    }
    
    /**
     * Constructeur avec limite de lignes et taille de batch
     * @param rowLimit Nombre maximum de lignes à charger
     * @param batchSize Taille des batchs pour le chargement
     */
    public ParquetLoadOptions(long rowLimit, int batchSize) {
        this.rowLimit = rowLimit;
        this.batchSize = batchSize;
    }
    
    /**
     * Crée une instance avec les options par défaut
     * @return Options par défaut
     */
    public static ParquetLoadOptions defaults() {
        return new ParquetLoadOptions();
    }
    
    /**
     * Obtient la limite de lignes
     * @return Limite de lignes (-1 pour illimité)
     */
    public long getRowLimit() {
        return rowLimit;
    }
    
    /**
     * Définit la limite de lignes
     * @param rowLimit Limite de lignes (-1 pour illimité)
     */
    public ParquetLoadOptions setRowLimit(long rowLimit) {
        this.rowLimit = rowLimit;
        return this;
    }
    
    /**
     * Obtient la taille des batchs
     * @return Taille des batchs
     */
    public int getBatchSize() {
        return batchSize;
    }
    
    /**
     * Définit la taille des batchs
     * @param batchSize Taille des batchs
     */
    public ParquetLoadOptions setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }
    
    /**
     * Obtient le timeout en secondes
     * @return Timeout en secondes (0 pour illimité)
     */
    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }
    
    /**
     * Définit le timeout en secondes
     * @param timeoutSeconds Timeout en secondes (0 pour illimité)
     */
    public ParquetLoadOptions setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
        return this;
    }
    
    /**
     * Indique si l'accès direct aux colonnes est utilisé
     * @return true si l'accès direct est utilisé
     */
    public boolean isUseDirectAccess() {
        return useDirectAccess;
    }
    
    /**
     * Définit si l'accès direct aux colonnes doit être utilisé
     * @param useDirectAccess true pour utiliser l'accès direct
     */
    public ParquetLoadOptions setUseDirectAccess(boolean useDirectAccess) {
        this.useDirectAccess = useDirectAccess;
        return this;
    }
    
    /**
     * Obtient le niveau de parallélisme
     * @return Nombre de threads parallèles
     */
    public int getParallelism() {
        return parallelism;
    }
    
    /**
     * Définit le niveau de parallélisme
     * @param parallelism Nombre de threads parallèles
     */
    public ParquetLoadOptions setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }
    
    /**
     * Définit les options de filtrage pour la distribution
     * @param filterOptions Options de filtrage (par exemple, nodeIndex et nodeCount pour filtrage modulo)
     * @return L'instance courante pour chaînage
     */
    public ParquetLoadOptions setFilterOptions(Map<String, Object> filterOptions) {
        this.filterOptions = filterOptions;
        return this;
    }
    
    /**
     * Obtient les options de filtrage
     * @return Options de filtrage ou null si non définies
     */
    public Map<String, Object> getFilterOptions() {
        return filterOptions;
    }
    
    /**
     * Obtient le nombre de lignes à sauter au début du fichier
     * @return Nombre de lignes à sauter
     */
    public int getSkipRows() {
        return skipRows;
    }
    
    /**
     * Définit le nombre de lignes à sauter au début du fichier
     * Utile pour charger une plage spécifique lors d'un chargement distribué
     * @param skipRows Nombre de lignes à sauter
     * @return L'instance courante pour chaînage
     */
    public ParquetLoadOptions setSkipRows(int skipRows) {
        this.skipRows = skipRows;
        return this;
    }
}
