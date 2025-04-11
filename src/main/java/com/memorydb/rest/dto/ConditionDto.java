package com.memorydb.rest.dto;

import com.memorydb.query.Condition;

/**
 * DTO pour les conditions de requête
 */
public class ConditionDto {
    private String columnName;
    private String operator;
    private Object value;
    
    /**
     * Constructeur par défaut pour Jackson
     */
    public ConditionDto() {
    }
    
    /**
     * Constructeur
     * @param columnName Le nom de la colonne
     * @param operator L'opérateur
     * @param value La valeur
     */
    public ConditionDto(String columnName, String operator, Object value) {
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
    }
    
    /**
     * Obtient le nom de la colonne
     * @return Le nom de la colonne
     */
    public String getColumnName() {
        return columnName;
    }
    
    /**
     * Définit le nom de la colonne
     * @param columnName Le nom de la colonne
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
    
    /**
     * Obtient l'opérateur
     * @return L'opérateur
     */
    public String getOperator() {
        return operator;
    }
    
    /**
     * Définit l'opérateur
     * @param operator L'opérateur
     */
    public void setOperator(String operator) {
        this.operator = operator;
    }
    
    /**
     * Obtient la valeur
     * @return La valeur
     */
    public Object getValue() {
        return value;
    }
    
    /**
     * Définit la valeur
     * @param value La valeur
     */
    public void setValue(Object value) {
        this.value = value;
    }
    
    /**
     * Convertit le DTO en objet Condition
     * @return L'objet Condition
     */
    public Condition toCondition() {
        // Gestion des cas spéciaux IS NULL et IS NOT NULL
        if ("IS_NULL".equals(operator)) {
            return Condition.isNull(columnName);
        } else if ("IS_NOT_NULL".equals(operator)) {
            return Condition.isNotNull(columnName);
        }
        
        // Conversion de l'opérateur
        Condition.Operator conditionOperator = Condition.Operator.valueOf(operator);
        return new Condition(columnName, conditionOperator, value);
    }
} 