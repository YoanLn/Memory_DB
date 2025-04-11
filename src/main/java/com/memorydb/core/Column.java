package com.memorydb.core;

import com.memorydb.common.DataType;

/**
 * Représente une colonne dans une table
 */
public class Column {
    private final String name;
    private final DataType type;
    private final boolean nullable;
    
    /**
     * Crée une nouvelle colonne
     * @param name Le nom de la colonne
     * @param type Le type de données de la colonne
     * @param nullable Indique si la colonne peut contenir des valeurs nulles
     */
    public Column(String name, DataType type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }
    
    /**
     * Crée une nouvelle colonne non-nullable
     * @param name Le nom de la colonne
     * @param type Le type de données de la colonne
     */
    public Column(String name, DataType type) {
        this(name, type, false);
    }
    
    /**
     * Obtient le nom de la colonne
     * @return Le nom de la colonne
     */
    public String getName() {
        return name;
    }
    
    /**
     * Obtient le type de données de la colonne
     * @return Le type de données
     */
    public DataType getType() {
        return type;
    }
    
    /**
     * Vérifie si la colonne peut contenir des valeurs nulles
     * @return true si la colonne peut contenir des valeurs nulles
     */
    public boolean isNullable() {
        return nullable;
    }
    
    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", nullable=" + nullable +
                '}';
    }
} 