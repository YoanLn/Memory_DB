package com.memorydb.query;

/**
 * Fonctions d'agrégation supportées
 */
public enum AggregateFunction {
    COUNT("COUNT"),
    SUM("SUM"),
    AVG("AVG"),
    MIN("MIN"),
    MAX("MAX");
    
    private final String name;
    
    AggregateFunction(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    /**
     * Convertit une chaîne en fonction d'agrégation
     * @param name Le nom de la fonction
     * @return La fonction d'agrégation
     */
    public static AggregateFunction fromString(String name) {
        for (AggregateFunction function : values()) {
            if (function.name.equalsIgnoreCase(name)) {
                return function;
            }
        }
        throw new IllegalArgumentException("Fonction d'agrégation inconnue: " + name);
    }
} 