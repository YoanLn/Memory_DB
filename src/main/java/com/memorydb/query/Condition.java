package com.memorydb.query;

import com.memorydb.common.DataType;
import com.memorydb.core.Column;
import com.memorydb.storage.ColumnStore;

import java.util.List;
import java.util.Arrays;

/**
 * Représente une condition dans une clause WHERE
 */
public class Condition {
    
    /**
     * Type d'opérateur
     */
    public enum Operator {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        LESS_THAN_OR_EQUALS,
        GREATER_THAN,
        GREATER_THAN_OR_EQUALS,
        LIKE,
        CONTAINS,
        STARTS_WITH,
        ENDS_WITH,
        IN,
        NOT_IN,
        BETWEEN,
        IS_NULL,
        IS_NOT_NULL
    }
    
    private final String columnName;
    private final Operator operator;
    private final Object value;
    
    /**
     * Crée une nouvelle condition
     * @param columnName Le nom de la colonne
     * @param operator L'opérateur
     * @param value La valeur
     */
    public Condition(String columnName, Operator operator, Object value) {
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
    }
    
    /**
     * Crée une nouvelle condition IS NULL
     * @param columnName Le nom de la colonne
     * @return La condition
     */
    public static Condition isNull(String columnName) {
        return new Condition(columnName, Operator.IS_NULL, null);
    }
    
    /**
     * Crée une nouvelle condition IS NOT NULL
     * @param columnName Le nom de la colonne
     * @return La condition
     */
    public static Condition isNotNull(String columnName) {
        return new Condition(columnName, Operator.IS_NOT_NULL, null);
    }
    
    /**
     * Crée une nouvelle condition CONTAINS
     * @param columnName Le nom de la colonne
     * @param substring La sous-chaîne à rechercher
     * @return La condition
     */
    public static Condition contains(String columnName, String substring) {
        return new Condition(columnName, Operator.CONTAINS, substring);
    }
    
    /**
     * Crée une nouvelle condition STARTS_WITH
     * @param columnName Le nom de la colonne
     * @param prefix Le préfixe à rechercher
     * @return La condition
     */
    public static Condition startsWith(String columnName, String prefix) {
        return new Condition(columnName, Operator.STARTS_WITH, prefix);
    }
    
    /**
     * Crée une nouvelle condition ENDS_WITH
     * @param columnName Le nom de la colonne
     * @param suffix Le suffixe à rechercher
     * @return La condition
     */
    public static Condition endsWith(String columnName, String suffix) {
        return new Condition(columnName, Operator.ENDS_WITH, suffix);
    }
    
    /**
     * Crée une nouvelle condition IN
     * @param columnName Le nom de la colonne
     * @param values La liste des valeurs
     * @return La condition
     */
    public static Condition in(String columnName, List<Object> values) {
        return new Condition(columnName, Operator.IN, values);
    }
    
    /**
     * Crée une nouvelle condition NOT IN
     * @param columnName Le nom de la colonne
     * @param values La liste des valeurs
     * @return La condition
     */
    public static Condition notIn(String columnName, List<Object> values) {
        return new Condition(columnName, Operator.NOT_IN, values);
    }
    
    /**
     * Crée une nouvelle condition BETWEEN
     * @param columnName Le nom de la colonne
     * @param minValue La valeur minimale (inclusive)
     * @param maxValue La valeur maximale (inclusive)
     * @return La condition
     */
    public static Condition between(String columnName, Object minValue, Object maxValue) {
        return new Condition(columnName, Operator.BETWEEN, Arrays.asList(minValue, maxValue));
    }
    
    /**
     * Obtient le nom de la colonne
     * @return Le nom de la colonne
     */
    public String getColumnName() {
        return columnName;
    }
    
    /**
     * Obtient l'opérateur
     * @return L'opérateur
     */
    public Operator getOperator() {
        return operator;
    }
    
    /**
     * Obtient la valeur
     * @return La valeur
     */
    public Object getValue() {
        return value;
    }
    
    /**
     * Évalue la condition pour une ligne donnée
     * @param row L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return true si la condition est satisfaite, false sinon
     */
    @SuppressWarnings("unchecked")
    public boolean evaluate(int row, ColumnStore columnStore) {
        Column column = columnStore.getColumn();
        DataType type = column.getType();
        
        // Cas spécial pour IS NULL et IS NOT NULL
        if (operator == Operator.IS_NULL) {
            return columnStore.isNull(row);
        }
        if (operator == Operator.IS_NOT_NULL) {
            return !columnStore.isNull(row);
        }
        
        // Si la valeur est null, la condition échoue (sauf pour IS NULL/IS NOT NULL traités ci-dessus)
        if (columnStore.isNull(row)) {
            return false;
        }
        
        // Évalue la condition en fonction du type
        switch (type) {
            case INTEGER:
                return evaluateInt(row, columnStore);
            case LONG:
                return evaluateLong(row, columnStore);
            case FLOAT:
                return evaluateFloat(row, columnStore);
            case DOUBLE:
                return evaluateDouble(row, columnStore);
            case BOOLEAN:
                return evaluateBoolean(row, columnStore);
            case STRING:
                return evaluateString(row, columnStore);
            case DATE:
            case TIMESTAMP:
                return evaluateDate(row, columnStore);
            default:
                throw new IllegalArgumentException("Type non supporté: " + type);
        }
    }
    
    /**
     * Évalue la condition pour une valeur entière
     * @param row L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return true si la condition est satisfaite, false sinon
     */
    private boolean evaluateInt(int row, ColumnStore columnStore) {
        int rowValue = columnStore.getInt(row);
        
        switch (operator) {
            case EQUALS:
                int compareValue = ((Number) value).intValue();
                return rowValue == compareValue;
            case NOT_EQUALS:
                int compareValue2 = ((Number) value).intValue();
                return rowValue != compareValue2;
            case LESS_THAN:
                int compareValue3 = ((Number) value).intValue();
                return rowValue < compareValue3;
            case LESS_THAN_OR_EQUALS:
                int compareValue4 = ((Number) value).intValue();
                return rowValue <= compareValue4;
            case GREATER_THAN:
                int compareValue5 = ((Number) value).intValue();
                return rowValue > compareValue5;
            case GREATER_THAN_OR_EQUALS:
                int compareValue6 = ((Number) value).intValue();
                return rowValue >= compareValue6;
            case IN:
                @SuppressWarnings("unchecked")
                List<Object> values = (List<Object>) value;
                for (Object val : values) {
                    if (val instanceof Number && ((Number) val).intValue() == rowValue) {
                        return true;
                    }
                }
                return false;
            case NOT_IN:
                @SuppressWarnings("unchecked")
                List<Object> values2 = (List<Object>) value;
                for (Object val : values2) {
                    if (val instanceof Number && ((Number) val).intValue() == rowValue) {
                        return false;
                    }
                }
                return true;
            case BETWEEN:
                @SuppressWarnings("unchecked")
                List<Object> range = (List<Object>) value;
                int minValue = ((Number) range.get(0)).intValue();
                int maxValue = ((Number) range.get(1)).intValue();
                return rowValue >= minValue && rowValue <= maxValue;
            default:
                throw new IllegalArgumentException("Opérateur non supporté pour INTEGER: " + operator);
        }
    }
    
    /**
     * Évalue la condition pour une valeur longue
     * @param row L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return true si la condition est satisfaite, false sinon
     */
    private boolean evaluateLong(int row, ColumnStore columnStore) {
        long rowValue = columnStore.getLong(row);
        
        switch (operator) {
            case EQUALS:
                long compareValue = ((Number) value).longValue();
                return rowValue == compareValue;
            case NOT_EQUALS:
                long compareValue2 = ((Number) value).longValue();
                return rowValue != compareValue2;
            case LESS_THAN:
                long compareValue3 = ((Number) value).longValue();
                return rowValue < compareValue3;
            case LESS_THAN_OR_EQUALS:
                long compareValue4 = ((Number) value).longValue();
                return rowValue <= compareValue4;
            case GREATER_THAN:
                long compareValue5 = ((Number) value).longValue();
                return rowValue > compareValue5;
            case GREATER_THAN_OR_EQUALS:
                long compareValue6 = ((Number) value).longValue();
                return rowValue >= compareValue6;
            case IN:
                @SuppressWarnings("unchecked")
                List<Object> values = (List<Object>) value;
                for (Object val : values) {
                    if (val instanceof Number && ((Number) val).longValue() == rowValue) {
                        return true;
                    }
                }
                return false;
            case NOT_IN:
                @SuppressWarnings("unchecked")
                List<Object> values2 = (List<Object>) value;
                for (Object val : values2) {
                    if (val instanceof Number && ((Number) val).longValue() == rowValue) {
                        return false;
                    }
                }
                return true;
            case BETWEEN:
                @SuppressWarnings("unchecked")
                List<Object> range = (List<Object>) value;
                long minValue = ((Number) range.get(0)).longValue();
                long maxValue = ((Number) range.get(1)).longValue();
                return rowValue >= minValue && rowValue <= maxValue;
            default:
                throw new IllegalArgumentException("Opérateur non supporté pour LONG: " + operator);
        }
    }
    
    /**
     * Évalue la condition pour une valeur float
     * @param row L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return true si la condition est satisfaite, false sinon
     */
    private boolean evaluateFloat(int row, ColumnStore columnStore) {
        float rowValue = columnStore.getFloat(row);
        float compareValue = ((Number) value).floatValue();
        
        switch (operator) {
            case EQUALS:
                return rowValue == compareValue;
            case NOT_EQUALS:
                return rowValue != compareValue;
            case LESS_THAN:
                return rowValue < compareValue;
            case LESS_THAN_OR_EQUALS:
                return rowValue <= compareValue;
            case GREATER_THAN:
                return rowValue > compareValue;
            case GREATER_THAN_OR_EQUALS:
                return rowValue >= compareValue;
            default:
                throw new IllegalArgumentException("Opérateur non supporté pour FLOAT: " + operator);
        }
    }
    
    /**
     * Évalue la condition pour une valeur double
     * @param row L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return true si la condition est satisfaite, false sinon
     */
    private boolean evaluateDouble(int row, ColumnStore columnStore) {
        double rowValue = columnStore.getDouble(row);
        
        switch (operator) {
            case EQUALS:
                double compareValue = ((Number) value).doubleValue();
                return rowValue == compareValue;
            case NOT_EQUALS:
                double compareValue2 = ((Number) value).doubleValue();
                return rowValue != compareValue2;
            case LESS_THAN:
                double compareValue3 = ((Number) value).doubleValue();
                return rowValue < compareValue3;
            case LESS_THAN_OR_EQUALS:
                double compareValue4 = ((Number) value).doubleValue();
                return rowValue <= compareValue4;
            case GREATER_THAN:
                double compareValue5 = ((Number) value).doubleValue();
                return rowValue > compareValue5;
            case GREATER_THAN_OR_EQUALS:
                double compareValue6 = ((Number) value).doubleValue();
                return rowValue >= compareValue6;
            case IN:
                @SuppressWarnings("unchecked")
                List<Object> values = (List<Object>) value;
                for (Object val : values) {
                    if (val instanceof Number && ((Number) val).doubleValue() == rowValue) {
                        return true;
                    }
                }
                return false;
            case NOT_IN:
                @SuppressWarnings("unchecked")
                List<Object> values2 = (List<Object>) value;
                for (Object val : values2) {
                    if (val instanceof Number && ((Number) val).doubleValue() == rowValue) {
                        return false;
                    }
                }
                return true;
            case BETWEEN:
                @SuppressWarnings("unchecked")
                List<Object> range = (List<Object>) value;
                double minValue = ((Number) range.get(0)).doubleValue();
                double maxValue = ((Number) range.get(1)).doubleValue();
                return rowValue >= minValue && rowValue <= maxValue;
            default:
                throw new IllegalArgumentException("Opérateur non supporté pour DOUBLE: " + operator);
        }
    }
    
    /**
     * Évalue la condition pour une valeur booléenne
     * @param row L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return true si la condition est satisfaite, false sinon
     */
    private boolean evaluateBoolean(int row, ColumnStore columnStore) {
        boolean rowValue = columnStore.getBoolean(row);
        boolean compareValue = (Boolean) value;
        
        switch (operator) {
            case EQUALS:
                return rowValue == compareValue;
            case NOT_EQUALS:
                return rowValue != compareValue;
            default:
                throw new IllegalArgumentException("Opérateur non supporté pour BOOLEAN: " + operator);
        }
    }
    
    /**
     * Évalue la condition pour une chaîne
     * @param row L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return true si la condition est satisfaite, false sinon
     */
    private boolean evaluateString(int row, ColumnStore columnStore) {
        String rowValue = columnStore.getString(row);
        String compareValue = (String) value;
        
        switch (operator) {
            case EQUALS:
                return rowValue.equals(compareValue);
            case NOT_EQUALS:
                return !rowValue.equals(compareValue);
            case LIKE:
                return matchesLike(rowValue, compareValue);
            case CONTAINS:
                return rowValue.contains(compareValue);
            case STARTS_WITH:
                return rowValue.startsWith(compareValue);
            case ENDS_WITH:
                return rowValue.endsWith(compareValue);
            case IN:
                return ((List<Object>) value).contains(rowValue);
            case NOT_IN:
                return !((List<Object>) value).contains(rowValue);
            default:
                throw new IllegalArgumentException("Opérateur non supporté pour STRING: " + operator);
        }
    }
    
    /**
     * Évalue la condition pour une date
     * @param row L'index de la ligne
     * @param columnStore Le stockage de colonne
     * @return true si la condition est satisfaite, false sinon
     */
    private boolean evaluateDate(int row, ColumnStore columnStore) {
        long rowValue = columnStore.getDate(row);
        long compareValue = ((Number) value).longValue();
        
        switch (operator) {
            case EQUALS:
                return rowValue == compareValue;
            case NOT_EQUALS:
                return rowValue != compareValue;
            case LESS_THAN:
                return rowValue < compareValue;
            case LESS_THAN_OR_EQUALS:
                return rowValue <= compareValue;
            case GREATER_THAN:
                return rowValue > compareValue;
            case GREATER_THAN_OR_EQUALS:
                return rowValue >= compareValue;
            case BETWEEN:
                List<Object> range = (List<Object>) value;
                long minValue = ((Number) range.get(0)).longValue();
                long maxValue = ((Number) range.get(1)).longValue();
                return rowValue >= minValue && rowValue <= maxValue;
            default:
                throw new IllegalArgumentException("Opérateur non supporté pour DATE/TIMESTAMP: " + operator);
        }
    }
    
    /**
     * Vérifie si une chaîne correspond à un modèle LIKE
     * @param value La chaîne à vérifier
     * @param pattern Le modèle LIKE
     * @return true si la chaîne correspond au modèle
     */
    private boolean matchesLike(String value, String pattern) {
        // Conversion du modèle LIKE SQL en expression régulière
        String regex = pattern.replace("%", ".*").replace("_", ".");
        return value.matches(regex);
    }
} 