package com.memorydb.rest.dto;

/**
 * DTO pour représenter une colonne de tri avec sa direction
 */
public class OrderByDto {
    private String column;
    private boolean ascending;
    
    /**
     * Constructeur par défaut
     */
    public OrderByDto() {
    }
    
    /**
     * Constructeur
     * @param column La colonne à trier
     * @param ascending true pour un tri ascendant, false pour descendant
     */
    public OrderByDto(String column, boolean ascending) {
        this.column = column;
        this.ascending = ascending;
    }
    
    /**
     * Obtient le nom de la colonne
     * @return Le nom de la colonne
     */
    public String getColumn() {
        return column;
    }
    
    /**
     * Définit le nom de la colonne
     * @param column Le nom de la colonne
     */
    public void setColumn(String column) {
        this.column = column;
    }
    
    /**
     * Vérifie si le tri est ascendant
     * @return true pour un tri ascendant, false pour descendant
     */
    public boolean isAscending() {
        return ascending;
    }
    
    /**
     * Définit si le tri est ascendant
     * @param ascending true pour un tri ascendant, false pour descendant
     */
    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }
}
