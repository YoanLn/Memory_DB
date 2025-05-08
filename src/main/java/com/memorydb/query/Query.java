package com.memorydb.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Représente une requête SQL-like
 */
public class Query {
    private final String tableName;
    private final List<String> selectColumns;
    private final List<Condition> conditions;
    private final List<String> groupByColumns;
    private final Map<String, AggregateFunction> aggregateFunctions;
    // Old single column order by (for backwards compatibility)
    private String orderBy;
    private boolean orderByAscending;
    
    // New multi-column order by support
    private final List<OrderByColumn> orderByColumns;
    private int limit;
    
    /**
     * Crée une nouvelle requête
     * @param tableName Le nom de la table
     */
    public Query(String tableName) {
        this.tableName = tableName;
        this.selectColumns = new ArrayList<>();
        this.conditions = new ArrayList<>();
        this.groupByColumns = new ArrayList<>();
        this.aggregateFunctions = new HashMap<>();
        this.orderBy = null;
        this.orderByAscending = true;
        this.orderByColumns = new ArrayList<>();
        this.limit = -1;
    }
    
    /**
     * Obtient le nom de la table
     * @return Le nom de la table
     */
    public String getTableName() {
        return tableName;
    }
    
    /**
     * Ajoute une colonne à la liste des colonnes à sélectionner
     * @param columnName Le nom de la colonne
     * @return Cette requête
     */
    public Query select(String columnName) {
        selectColumns.add(columnName);
        return this;
    }
    
    /**
     * Ajoute une condition à la requête
     * @param condition La condition
     * @return Cette requête
     */
    public Query where(Condition condition) {
        conditions.add(condition);
        return this;
    }
    
    /**
     * Ajoute une colonne de regroupement
     * @param columnName Le nom de la colonne
     * @return Cette requête
     */
    public Query groupBy(String columnName) {
        groupByColumns.add(columnName);
        return this;
    }
    
    /**
     * Ajoute une fonction d'agrégation
     * @param columnName Le nom de la colonne
     * @param function La fonction d'agrégation
     * @return Cette requête
     */
    public Query aggregate(String columnName, AggregateFunction function) {
        aggregateFunctions.put(columnName, function);
        return this;
    }
    
    /**
     * Définit la colonne de tri (ordre ascendant)
     * @param columnName Le nom de la colonne
     * @return Cette requête
     */
    public Query orderByAsc(String columnName) {
        this.orderBy = columnName;
        this.orderByAscending = true;
        
        // Also update the new multi-column order by
        this.orderByColumns.clear();
        this.orderByColumns.add(new OrderByColumn(columnName, true));
        
        return this;
    }
    
    /**
     * Définit la colonne de tri (ordre descendant)
     * @param columnName Le nom de la colonne
     * @return Cette requête
     */
    public Query orderByDesc(String columnName) {
        this.orderBy = columnName;
        this.orderByAscending = false;
        
        // Also update the new multi-column order by
        this.orderByColumns.clear();
        this.orderByColumns.add(new OrderByColumn(columnName, false));
        
        return this;
    }
    
    /**
     * Ajoute une colonne de tri (ordre ascendant) à la liste des colonnes de tri
     * Cela permet de trier par plusieurs colonnes
     * @param columnName Le nom de la colonne
     * @return Cette requête
     */
    public Query addOrderByAsc(String columnName) {
        this.orderByColumns.add(new OrderByColumn(columnName, true));
        
        // Update single column orderBy if this is the first column
        if (this.orderByColumns.size() == 1) {
            this.orderBy = columnName;
            this.orderByAscending = true;
        }
        
        return this;
    }
    
    /**
     * Ajoute une colonne de tri (ordre descendant) à la liste des colonnes de tri
     * Cela permet de trier par plusieurs colonnes
     * @param columnName Le nom de la colonne
     * @return Cette requête
     */
    public Query addOrderByDesc(String columnName) {
        this.orderByColumns.add(new OrderByColumn(columnName, false));
        
        // Update single column orderBy if this is the first column
        if (this.orderByColumns.size() == 1) {
            this.orderBy = columnName;
            this.orderByAscending = false;
        }
        
        return this;
    }
    
    /**
     * Définit la limite de résultats
     * @param limit La limite
     * @return Cette requête
     */
    public Query limit(int limit) {
        this.limit = limit;
        return this;
    }
    
    /**
     * Obtient la liste des colonnes à sélectionner
     * @return Les colonnes
     */
    public List<String> getColumns() {
        return new ArrayList<>(selectColumns);
    }
    
    /**
     * Obtient la liste des conditions
     * @return Les conditions
     */
    public List<Condition> getConditions() {
        return new ArrayList<>(conditions);
    }
    
    /**
     * Obtient la liste des colonnes de regroupement
     * @return Les colonnes
     */
    public List<String> getGroupByColumns() {
        return new ArrayList<>(groupByColumns);
    }
    
    /**
     * Obtient les fonctions d'agrégation
     * @return Les fonctions d'agrégation
     */
    public Map<String, AggregateFunction> getAggregateFunctions() {
        return new HashMap<>(aggregateFunctions);
    }
    
    /**
     * Obtient la colonne de tri principale (pour compatibilité descendante)
     * @return La colonne
     */
    public String getOrderBy() {
        return orderBy;
    }
    
    /**
     * Vérifie si le tri principal est ascendant (pour compatibilité descendante)
     * @return true si le tri est ascendant
     */
    public boolean isOrderByAscending() {
        return orderByAscending;
    }
    
    /**
     * Obtient la liste des colonnes de tri avec leur direction
     * @return La liste des colonnes de tri
     */
    public List<OrderByColumn> getOrderByColumns() {
        return new ArrayList<>(orderByColumns);
    }
    
    /**
     * Obtient la limite de résultats
     * @return La limite
     */
    public int getLimit() {
        return limit;
    }
} 