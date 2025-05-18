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
    private final Map<String, AggregateDefinition> aggregateFunctions;
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
        this.aggregateFunctions = new HashMap<String, AggregateDefinition>();
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
     * Ajoute une fonction d'agrégation.
     * L'alias est utilisé comme clé et doit permettre d'inférer la colonne cible.
     * Ex: "SUM_fare_amount" où "SUM" est la fonction et "fare_amount" la colonne cible.
     * Pour COUNT(*), utiliser l'alias "COUNT_STAR".
     *
     * @param alias    L'alias pour la fonction d'agrégation (ex: "SUM_fare_amount").
     * @param function Le type de fonction d'agrégation (ex: AggregateFunction.SUM).
     * @return Cette requête.
     * @throws IllegalArgumentException si la colonne cible ne peut pas être inférée de l'alias,
     *                                ou si l'alias ou la fonction est null.
     */
    public Query aggregate(String alias, AggregateFunction function) {
        String targetColumn = inferTargetColumn(alias, function);
        AggregateDefinition aggDef = new AggregateDefinition(alias, function, targetColumn);
        aggregateFunctions.put(alias, aggDef);
        return this;
    }

    /**
     * Infère la colonne cible à partir de l'alias de l'agrégation et de la fonction.
     * Attend un alias au format "NOMFONCTION_NOMCOLONNE" (ex: "SUM_fare_amount")
     * ou "COUNT_STAR" pour COUNT(*).
     *
     * @param alias    L'alias de l'agrégation.
     * @param function Le type de fonction d'agrégation.
     * @return Le nom de la colonne cible.
     * @throws IllegalArgumentException si la colonne cible ne peut pas être inférée,
     *                                ou si l'alias ou la fonction est null/vide.
     */
    private String inferTargetColumn(String alias, AggregateFunction function) {
        if (alias == null || alias.isEmpty()) {
            throw new IllegalArgumentException("Alias cannot be null or empty.");
        }
        if (function == null) {
            throw new IllegalArgumentException("AggregateFunction cannot be null.");
        }

        // Handle COUNT(*) specifically. Assume alias "COUNT_STAR" is used.
        if (alias.equalsIgnoreCase("COUNT_STAR")) {
            return "*";
        }

        // Handle simple "count" alias for COUNT(*)
        if (function == AggregateFunction.COUNT && alias.equalsIgnoreCase("count")) {
            return "*";
        }

        String functionNameInAliasPrefix;
        String inferredTargetColumn;

        int firstUnderscoreIndex = alias.indexOf('_');

        if (firstUnderscoreIndex > 0 && firstUnderscoreIndex < alias.length() - 1) {
            functionNameInAliasPrefix = alias.substring(0, firstUnderscoreIndex);
            inferredTargetColumn = alias.substring(firstUnderscoreIndex + 1);

            if (functionNameInAliasPrefix.equalsIgnoreCase(function.name())) {
                if (inferredTargetColumn.isEmpty()) {
                     throw new IllegalArgumentException(
                        String.format("Inferred target column is empty for alias '%s'. Expected format like 'FUNCTION_COLUMNNAME'.", alias)
                    );
                }
                return inferredTargetColumn;
            } else {
                throw new IllegalArgumentException(
                    String.format("Aggregate function name prefix in alias ('%s') does not match the provided function type ('%s') for alias '%s'. Expected format 'FUNCTION_COLUMNNAME'.",
                                  functionNameInAliasPrefix, function.name(), alias)
                );
            }
        }

        throw new IllegalArgumentException(
            String.format("Cannot infer target column from alias '%s'. Expected format 'FUNCTION_COLUMNNAME' (e.g., 'SUM_fare_amount') or 'COUNT_STAR'.", alias)
        );
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
     * Obtient les définitions des fonctions d'agrégation.
     * La clé de la map est l'alias de l'agrégation.
     *
     * @return Une map des alias vers leurs AggregateDefinition.
     */
    public Map<String, AggregateDefinition> getAggregateFunctions() {
        return new HashMap<String, AggregateDefinition>(aggregateFunctions);
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