package com.memorydb.rest.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.memorydb.query.AggregateFunction;
import com.memorydb.query.Condition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DTO pour les requêtes
 */
public class QueryDto {
    private String tableName;
    private List<String> columns;
    private List<ConditionDto> conditions;
    @JsonDeserialize(using = OrderByDeserializer.class)
    private List<OrderByDto> orderBy;
    // Keeping these for backward compatibility
    private String orderByColumn;
    private boolean orderByAscending;
    private int limit;
    private boolean distributed;
    // Added fields for GROUP BY and aggregation support
    private List<String> groupBy;
    private Map<String, String> aggregates; // format: {"column": "FUNCTION"}
    /**
     * Flag indiquant si la requête a déjà été transmise à un autre nœud.
     * Utilisé pour éviter les boucles infinies dans les requêtes distribuées.
     */
    private boolean forwardedQuery = false;
    
    /**
     * Constructeur par défaut
     */
    public QueryDto() {
        this.groupBy = new ArrayList<>();
        this.aggregates = new HashMap<>();
    }
    
    /**
     * Constructeur
     * @param tableName Le nom de la table
     * @param columns Les colonnes à sélectionner
     * @param conditions Les conditions de la requête
     * @param orderByColumn La colonne pour le tri (compatibilité descendante)
     * @param orderByAscending true pour un tri ascendant, false pour descendant
     * @param limit La limite de résultats
     */
    public QueryDto(String tableName, List<String> columns, List<ConditionDto> conditions, 
                   String orderByColumn, boolean orderByAscending, int limit) {
        this.tableName = tableName;
        this.columns = columns;
        this.conditions = conditions;
        this.orderByColumn = orderByColumn;
        this.orderByAscending = orderByAscending;
        this.limit = limit;
        this.distributed = false;
        this.groupBy = new ArrayList<>();
        this.aggregates = new HashMap<>();
        
        // Initialize orderBy list with the single column for backward compatibility
        if (orderByColumn != null && !orderByColumn.isEmpty()) {
            this.orderBy = new ArrayList<>();
            this.orderBy.add(new OrderByDto(orderByColumn, orderByAscending));
        } else {
            this.orderBy = new ArrayList<>();
        }
    }
    
    /**
     * Constructeur avec option distribuée
     * @param tableName Le nom de la table
     * @param columns Les colonnes à sélectionner
     * @param conditions Les conditions de la requête
     * @param orderByColumn La colonne pour le tri (compatibilité descendante)
     * @param orderByAscending true pour un tri ascendant, false pour descendant
     * @param limit La limite de résultats
     * @param distributed true pour exécuter la requête en mode distribué
     */
    public QueryDto(String tableName, List<String> columns, List<ConditionDto> conditions, 
                   String orderByColumn, boolean orderByAscending, int limit, boolean distributed) {
        this.tableName = tableName;
        this.columns = columns;
        this.conditions = conditions;
        this.orderByColumn = orderByColumn;
        this.orderByAscending = orderByAscending;
        this.limit = limit;
        this.distributed = distributed;
        this.groupBy = new ArrayList<>();
        this.aggregates = new HashMap<>();
        
        // Initialize orderBy list with the single column for backward compatibility
        if (orderByColumn != null && !orderByColumn.isEmpty()) {
            this.orderBy = new ArrayList<>();
            this.orderBy.add(new OrderByDto(orderByColumn, orderByAscending));
        } else {
            this.orderBy = new ArrayList<>();
        }
    }
    
    /**
     * Obtient le nom de la table
     * @return Le nom de la table
     */
    public String getTableName() {
        return tableName;
    }
    
    /**
     * Définit le nom de la table
     * @param tableName Le nom de la table
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    /**
     * Obtient les colonnes à sélectionner
     * @return Les colonnes
     */
    public List<String> getColumns() {
        return columns;
    }
    
    /**
     * Définit les colonnes à sélectionner
     * @param columns Les colonnes
     */
    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
    
    /**
     * Obtient les conditions de la requête
     * @return Les conditions
     */
    public List<ConditionDto> getConditions() {
        return conditions;
    }
    
    /**
     * Définit les conditions de la requête
     * @param conditions Les conditions
     */
    public void setConditions(List<ConditionDto> conditions) {
        this.conditions = conditions;
    }
    
    /**
     * Convertit les ConditionDto en Condition
     * @return La liste des conditions
     */
    public List<Condition> toConditions() {
        if (conditions == null || conditions.isEmpty()) {
            return Collections.emptyList(); // Utiliser une liste constante plutôt qu'une nouvelle instance
        }
        
        // Pré-allouer avec la taille exacte et éviter les streams pour cette opération simple
        List<Condition> result = new ArrayList<>(conditions.size());
        for (ConditionDto conditionDto : conditions) {
            result.add(conditionDto.toCondition());
        }
        return result;
    }
    
    /**
     * Obtient les colonnes pour le tri
     * @return Les colonnes de tri avec leurs directions
     */
    public List<OrderByDto> getOrderBy() {
        return orderBy;
    }
    
    /**
     * Définit les colonnes pour le tri
     * @param orderBy Les colonnes de tri avec leurs directions
     */
    public void setOrderBy(List<OrderByDto> orderBy) {
        this.orderBy = orderBy;
    }
    
    /**
     * Obtient la colonne pour le tri (compatibilité descendante)
     * @return La colonne
     */
    public String getOrderByColumn() {
        if (orderByColumn != null) {
            return orderByColumn;
        } else if (orderBy != null && !orderBy.isEmpty()) {
            return orderBy.get(0).getColumn();
        }
        return null;
    }
    
    /**
     * Définit la colonne pour le tri (compatibilité descendante)
     * @param orderByColumn La colonne
     */
    public void setOrderByColumn(String orderByColumn) {
        this.orderByColumn = orderByColumn;
        
        // Mise à jour optimisée pour éviter les allocations inutiles
        if (orderByColumn != null && !orderByColumn.isEmpty()) {
            if (this.orderBy == null) {
                this.orderBy = new ArrayList<>(1); // Capacité initiale exacte
                this.orderBy.add(new OrderByDto(orderByColumn, this.orderByAscending));
            } else if (this.orderBy.isEmpty()) {
                this.orderBy.add(new OrderByDto(orderByColumn, this.orderByAscending));
            } else {
                // Réutiliser l'objet existant si possible
                OrderByDto first = this.orderBy.get(0);
                if (this.orderBy.size() == 1 && first != null) {
                    first.setColumn(orderByColumn);
                    first.setAscending(this.orderByAscending);
                } else {
                    this.orderBy.clear();
                    this.orderBy.add(new OrderByDto(orderByColumn, this.orderByAscending));
                }
            }
        }
    }
    
    /**
     * Vérifie si le tri est ascendant
     * @return true si le tri est ascendant, false sinon
     */
    public boolean isOrderByAscending() {
        return orderByAscending;
    }
    
    /**
     * Définit si le tri est ascendant
     * @param orderByAscending true pour un tri ascendant, false pour descendant
     */
    public void setOrderByAscending(boolean orderByAscending) {
        this.orderByAscending = orderByAscending;
    }
    
    /**
     * Obtient la limite de résultats
     * @return La limite
     */
    public int getLimit() {
        return limit;
    }
    
    /**
     * Définit la limite de résultats
     * @param limit La limite
     */
    public void setLimit(int limit) {
        this.limit = limit;
    }
    
    /**
     * Vérifie si la requête doit être exécutée en mode distribué
     * @return true si la requête doit être distribuée
     */
    public boolean isDistributed() {
        return distributed;
    }
    
    /**
     * Définit si la requête doit être exécutée en mode distribué
     * @param distributed true pour exécuter en mode distribué
     */
    public void setDistributed(boolean distributed) {
        this.distributed = distributed;
    }
    
    public boolean isForwardedQuery() {
        return forwardedQuery;
    }
    
    public void setForwardedQuery(boolean forwardedQuery) {
        this.forwardedQuery = forwardedQuery;
    }
    
    /**
     * Get the GROUP BY columns
     * @return list of columns to group by
     */
    public List<String> getGroupBy() {
        return groupBy;
    }
    
    /**
     * Set the GROUP BY columns
     * @param groupBy list of columns to group by
     */
    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }
    
    /**
     * Get the aggregate functions mapping
     * @return map of column name to function name
     */
    public Map<String, String> getAggregates() {
        return aggregates;
    }
    
    /**
     * Set the aggregate functions mapping
     * @param aggregates map of column name to function name
     */
    public void setAggregates(Map<String, String> aggregates) {
        this.aggregates = aggregates;
    }
    
    /**
     * Helper method to convert the string-based aggregates map to the actual AggregateFunction enum types
     * @return map of column name to AggregateFunction enum
     */
    @JsonIgnore
    public Map<String, AggregateFunction> toAggregateFunctions() {
        if (aggregates == null || aggregates.isEmpty()) {
            return Collections.emptyMap();
        }
        
        Map<String, AggregateFunction> result = new HashMap<>();
        for (Map.Entry<String, String> entry : aggregates.entrySet()) {
            try {
                result.put(entry.getKey(), AggregateFunction.fromString(entry.getValue()));
            } catch (IllegalArgumentException e) {
                // Skip invalid function names
            }
        }
        return result;
    }
} 