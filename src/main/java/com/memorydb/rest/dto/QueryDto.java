package com.memorydb.rest.dto;

import com.memorydb.query.Condition;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DTO pour les requêtes
 */
public class QueryDto {
    private String tableName;
    private List<String> columns;
    private List<ConditionDto> conditions;
    private String orderBy;
    private boolean orderByAscending;
    private int limit;
    private boolean distributed;
    
    /**
     * Constructeur par défaut
     */
    public QueryDto() {
    }
    
    /**
     * Constructeur
     * @param tableName Le nom de la table
     * @param columns Les colonnes à sélectionner
     * @param conditions Les conditions de la requête
     * @param orderBy La colonne pour le tri
     * @param orderByAscending true pour un tri ascendant, false pour descendant
     * @param limit La limite de résultats
     */
    public QueryDto(String tableName, List<String> columns, List<ConditionDto> conditions, 
                   String orderBy, boolean orderByAscending, int limit) {
        this.tableName = tableName;
        this.columns = columns;
        this.conditions = conditions;
        this.orderBy = orderBy;
        this.orderByAscending = orderByAscending;
        this.limit = limit;
        this.distributed = false;
    }
    
    /**
     * Constructeur avec option distribuée
     * @param tableName Le nom de la table
     * @param columns Les colonnes à sélectionner
     * @param conditions Les conditions de la requête
     * @param orderBy La colonne pour le tri
     * @param orderByAscending true pour un tri ascendant, false pour descendant
     * @param limit La limite de résultats
     * @param distributed true pour exécuter la requête en mode distribué
     */
    public QueryDto(String tableName, List<String> columns, List<ConditionDto> conditions, 
                   String orderBy, boolean orderByAscending, int limit, boolean distributed) {
        this.tableName = tableName;
        this.columns = columns;
        this.conditions = conditions;
        this.orderBy = orderBy;
        this.orderByAscending = orderByAscending;
        this.limit = limit;
        this.distributed = distributed;
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
        if (conditions == null) {
            return new ArrayList<>();
        }
        return conditions.stream()
                .map(ConditionDto::toCondition)
                .collect(Collectors.toList());
    }
    
    /**
     * Obtient la colonne pour le tri
     * @return La colonne
     */
    public String getOrderBy() {
        return orderBy;
    }
    
    /**
     * Définit la colonne pour le tri
     * @param orderBy La colonne
     */
    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
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
} 