package com.memorydb.query;

/**
 * Représente une colonne de tri avec sa direction
 */
public class OrderByColumn {
    private final String columnName;
    private final boolean ascending;
    
    /**
     * Crée une nouvelle colonne de tri
     * @param columnName Le nom de la colonne
     * @param ascending true si le tri est ascendant, false sinon
     */
    public OrderByColumn(String columnName, boolean ascending) {
        this.columnName = columnName;
        this.ascending = ascending;
    }
    
    /**
     * Obtient le nom de la colonne
     * @return Le nom de la colonne
     */
    public String getColumnName() {
        return columnName;
    }
    
    /**
     * Vérifie si le tri est ascendant
     * @return true si le tri est ascendant, false sinon
     */
    public boolean isAscending() {
        return ascending;
    }
}
