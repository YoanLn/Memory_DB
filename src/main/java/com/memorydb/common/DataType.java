package com.memorydb.common;

/**
 * Énumération des types de données supportés par la base de données
 */
public enum DataType {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    STRING,
    DATE,
    TIMESTAMP;
    
    /**
     * Convertit une chaîne en DataType
     * @param type La chaîne à convertir
     * @return Le DataType correspondant
     */
    public static DataType fromString(String type) {
        return DataType.valueOf(type.toUpperCase());
    }
    
    /**
     * Vérifie si le type est numérique
     * @return true si le type est numérique
     */
    public boolean isNumeric() {
        return this == INTEGER || this == LONG || this == FLOAT || this == DOUBLE;
    }
    
    /**
     * Vérifie si le type est une date ou un timestamp
     * @return true si le type est une date ou un timestamp
     */
    public boolean isTemporal() {
        return this == DATE || this == TIMESTAMP;
    }
} 