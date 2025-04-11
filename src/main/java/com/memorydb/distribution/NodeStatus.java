package com.memorydb.distribution;

/**
 * Énumération des statuts possibles d'un nœud
 */
public enum NodeStatus {
    /**
     * Le nœud est en ligne et disponible
     */
    ONLINE,
    
    /**
     * Le nœud est temporairement hors ligne (aucun battement de cœur récent)
     */
    OFFLINE,
    
    /**
     * Le nœud est en cours de démarrage ou de redémarrage
     */
    STARTING,
    
    /**
     * Le nœud est en cours d'arrêt gracieux
     */
    SHUTTING_DOWN,
    
    /**
     * Le nœud est en cours de maintenance
     */
    MAINTENANCE,
    
    /**
     * Le nœud est en erreur
     */
    ERROR
} 