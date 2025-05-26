package com.memorydb.query;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parseur de requêtes SQL
 */
public class SQLParser {
    
    // Patrons de regex pour les différentes parties de la requête SQL
    private static final Pattern SELECT_PATTERN = Pattern.compile(
            "SELECT\\s+(.+?)\\s+FROM\\s+([\\w]+)(?:\\s+WHERE\\s+(.+?))?(?:\\s+GROUP\\s+BY\\s+(.+?))?(?:\\s+HAVING\\s+(.+?))?(?:\\s+ORDER\\s+BY\\s+(.+?))?(?:\\s+LIMIT\\s+(\\d+))?\\s*",
            Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern COLUMN_PATTERN = Pattern.compile(
            "(?:(\\w+)\\(([^\\)]+)\\)(?:\\s+AS\\s+([\\w]+))?)|([\\w]+)",
            Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern CONDITION_PATTERN = Pattern.compile(
            "([\\w]+)\\s*(=|!=|<>|<|<=|>|>=|LIKE|CONTAINS|STARTS_WITH|ENDS_WITH|IN|NOT_IN|BETWEEN|IS NULL|IS NOT NULL)\\s*(?:'([^']*)'|(\\d+)|\\(([^)]+)\\))?",
            Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern ORDER_BY_PATTERN = Pattern.compile(
            "([\\w]+)(?:\\s+(ASC|DESC))?",
            Pattern.CASE_INSENSITIVE 
    );
    
    /**
     * Parse une requête SQL
     * @param sql La requête SQL
     * @return L'objet Query correspondant
     */
    public Query parse(String sql) {
        Matcher matcher = SELECT_PATTERN.matcher(sql);
        
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Requête SQL invalide: " + sql);
        }
        
        String columnsStr = matcher.group(1);
        String tableName = matcher.group(2);
        String whereStr = matcher.group(3);
        String groupByStr = matcher.group(4);
        String havingStr = matcher.group(5);
        String orderByStr = matcher.group(6);
        String limitStr = matcher.group(7);
        
        Query query = new Query(tableName);
        
        // Parse les colonnes
        parseColumns(query, columnsStr);
        
        // Parse les conditions WHERE
        if (whereStr != null) {
            parseConditions(query, whereStr);
        }
        
        // Parse le GROUP BY
        if (groupByStr != null) {
            parseGroupBy(query, groupByStr);
        }
        
        // Parse le HAVING
        if (havingStr != null) {
            parseHaving(query, havingStr);
        }
        
        // Parse le ORDER BY
        if (orderByStr != null) {
            parseOrderBy(query, orderByStr);
        }
        
        // Ajoute la limite
        if (limitStr != null) {
            query.limit(Integer.parseInt(limitStr));
        }
        
        return query;
    }
    
    /**
     * Parse la clause SELECT
     * @param query La requête
     * @param columnsStr La chaîne des colonnes
     */
    private void parseColumns(Query query, String columnsStr) {
        String[] columns = columnsStr.split("\\s*,\\s*");
        
        for (String column : columns) {
            Matcher matcher = COLUMN_PATTERN.matcher(column.trim());
            
            if (matcher.matches()) {
                String functionName = matcher.group(1);
                String columnName = matcher.group(2);
                String alias = matcher.group(3);
                String simpleColumn = matcher.group(4);
                
                if (functionName != null && columnName != null) {
                    // C'est une fonction d'agrégation
                    AggregateFunction function = AggregateFunction.fromString(functionName);
                    query.aggregate(columnName, function);
                } else if (simpleColumn != null) {
                    // C'est une colonne simple
                    query.select(simpleColumn);
                }
            } else {
                throw new IllegalArgumentException("Colonne invalide: " + column);
            }
        }
    }
    
    /**
     * Parse la clause WHERE
     * @param query La requête
     * @param whereStr La chaîne des conditions
     */
    private void parseConditions(Query query, String whereStr) {
        // Pour simplifier, on ne gère que les conditions avec AND
        String[] conditions = whereStr.split("\\s+AND\\s+");
        
        for (String condition : conditions) {
            Matcher matcher = CONDITION_PATTERN.matcher(condition.trim());
            
            if (matcher.matches()) {
                String columnName = matcher.group(1);
                String operator = matcher.group(2).toUpperCase();
                String stringValue = matcher.group(3);
                String numericValue = matcher.group(4);
                String listValue = matcher.group(5);
                
                // Détermine l'opérateur
                Condition.Operator conditionOperator;
                switch (operator) {
                    case "=":
                        conditionOperator = Condition.Operator.EQUALS;
                        break;
                    case "!=":
                    case "<>":
                        conditionOperator = Condition.Operator.NOT_EQUALS;
                        break;
                    case "<":
                        conditionOperator = Condition.Operator.LESS_THAN;
                        break;
                    case "<=":
                        conditionOperator = Condition.Operator.LESS_THAN_OR_EQUALS;
                        break;
                    case ">":
                        conditionOperator = Condition.Operator.GREATER_THAN;
                        break;
                    case ">=":
                        conditionOperator = Condition.Operator.GREATER_THAN_OR_EQUALS;
                        break;
                    case "LIKE":
                        conditionOperator = Condition.Operator.LIKE;
                        break;
                    case "CONTAINS":
                        conditionOperator = Condition.Operator.CONTAINS;
                        break;
                    case "STARTS_WITH":
                        conditionOperator = Condition.Operator.STARTS_WITH;
                        break;
                    case "ENDS_WITH":
                        conditionOperator = Condition.Operator.ENDS_WITH;
                        break;
                    case "IN":
                        conditionOperator = Condition.Operator.IN;
                        break;
                    case "NOT_IN":
                        conditionOperator = Condition.Operator.NOT_IN;
                        break;
                    case "BETWEEN":
                        conditionOperator = Condition.Operator.BETWEEN;
                        break;
                    case "IS NULL":
                        query.where(Condition.isNull(columnName));
                        continue;
                    case "IS NOT NULL":
                        query.where(Condition.isNotNull(columnName));
                        continue;
                    default:
                        throw new IllegalArgumentException("Opérateur inconnu: " + operator);
                }
                
                // Détermine la valeur
                Object value = parseConditionValue(conditionOperator, stringValue, numericValue, listValue, condition);
                query.where(new Condition(columnName, conditionOperator, value));
            } else {
                throw new IllegalArgumentException("Condition invalide: " + condition);
            }
        }
    }
    
    /**
     * Parse la clause GROUP BY
     * @param query La requête
     * @param groupByStr La chaîne des colonnes de regroupement
     */
    private void parseGroupBy(Query query, String groupByStr) {
        String[] columns = groupByStr.split("\\s*,\\s*");
        
        for (String column : columns) {
            query.groupBy(column.trim());
        }
    }
    
    /**
     * Parse la clause HAVING
     * @param query La requête
     * @param havingStr La chaîne des conditions HAVING
     */
    private void parseHaving(Query query, String havingStr) {
        // Pour simplifier, on ne gère que les conditions avec AND
        String[] conditions = havingStr.split("\\s+AND\\s+");
        
        for (String condition : conditions) {
            Matcher matcher = CONDITION_PATTERN.matcher(condition.trim());
            
            if (matcher.matches()) {
                String columnName = matcher.group(1);
                String operator = matcher.group(2).toUpperCase();
                String stringValue = matcher.group(3);
                String numericValue = matcher.group(4);
                String listValue = matcher.group(5);
                
                // Détermine l'opérateur
                Condition.Operator conditionOperator;
                switch (operator) {
                    case "=":
                        conditionOperator = Condition.Operator.EQUALS;
                        break;
                    case "!=":
                    case "<>":
                        conditionOperator = Condition.Operator.NOT_EQUALS;
                        break;
                    case "<":
                        conditionOperator = Condition.Operator.LESS_THAN;
                        break;
                    case "<=":
                        conditionOperator = Condition.Operator.LESS_THAN_OR_EQUALS;
                        break;
                    case ">":
                        conditionOperator = Condition.Operator.GREATER_THAN;
                        break;
                    case ">=":
                        conditionOperator = Condition.Operator.GREATER_THAN_OR_EQUALS;
                        break;
                    case "LIKE":
                        conditionOperator = Condition.Operator.LIKE;
                        break;
                    case "CONTAINS":
                        conditionOperator = Condition.Operator.CONTAINS;
                        break;
                    case "STARTS_WITH":
                        conditionOperator = Condition.Operator.STARTS_WITH;
                        break;
                    case "ENDS_WITH":
                        conditionOperator = Condition.Operator.ENDS_WITH;
                        break;
                    case "IN":
                        conditionOperator = Condition.Operator.IN;
                        break;
                    case "NOT_IN":
                        conditionOperator = Condition.Operator.NOT_IN;
                        break;
                    case "BETWEEN":
                        conditionOperator = Condition.Operator.BETWEEN;
                        break;
                    case "IS NULL":
                        query.having(Condition.isNull(columnName));
                        continue;
                    case "IS NOT NULL":
                        query.having(Condition.isNotNull(columnName));
                        continue;
                    default:
                        throw new IllegalArgumentException("Opérateur HAVING inconnu: " + operator);
                }
                
                // Détermine la valeur
                Object value = parseConditionValue(conditionOperator, stringValue, numericValue, listValue, condition);
                query.having(new Condition(columnName, conditionOperator, value));
            } else {
                throw new IllegalArgumentException("Condition HAVING invalide: " + condition);
            }
        }
    }
    
    /**
     * Parse la clause ORDER BY
     * @param query La requête
     * @param orderByStr La chaîne des colonnes de tri
     */
    private void parseOrderBy(Query query, String orderByStr) {
        String[] columns = orderByStr.split("\\s*,\\s*");
        
        for (String column : columns) {
            Matcher matcher = ORDER_BY_PATTERN.matcher(column.trim());
            
            if (matcher.matches()) {
                String columnName = matcher.group(1);
                String direction = matcher.group(2);
                
                boolean ascending = direction == null || direction.equalsIgnoreCase("ASC");
                if (ascending) {
                    query.orderByAsc(columnName);
                } else {
                    query.orderByDesc(columnName);
                }
            } else {
                throw new IllegalArgumentException("Colonne ORDER BY invalide: " + column);
            }
        }
    }
    
    /**
     * Parse la valeur d'une condition en fonction de l'opérateur
     * @param operator L'opérateur de la condition
     * @param stringValue La valeur chaîne
     * @param numericValue La valeur numérique
     * @param listValue La valeur liste (pour IN, NOT_IN, BETWEEN)
     * @param condition La condition complète (pour les messages d'erreur)
     * @return La valeur parsée
     */
    private Object parseConditionValue(Condition.Operator operator, String stringValue, String numericValue, String listValue, String condition) {
        switch (operator) {
            case IN:
            case NOT_IN:
                if (listValue != null) {
                    // Parse la liste de valeurs: (val1, val2, val3)
                    String[] values = listValue.split("\\s*,\\s*");
                    java.util.List<Object> valueList = new java.util.ArrayList<>();
                    for (String val : values) {
                        val = val.trim();
                        if (val.startsWith("'") && val.endsWith("'")) {
                            // Valeur chaîne
                            valueList.add(val.substring(1, val.length() - 1));
                        } else {
                            // Valeur numérique
                            try {
                                valueList.add(Integer.parseInt(val));
                            } catch (NumberFormatException e) {
                                try {
                                    valueList.add(Double.parseDouble(val));
                                } catch (NumberFormatException e2) {
                                    valueList.add(val); // Traiter comme chaîne
                                }
                            }
                        }
                    }
                    return valueList;
                } else {
                    throw new IllegalArgumentException("Liste de valeurs manquante pour " + operator + ": " + condition);
                }
                
            case BETWEEN:
                if (listValue != null) {
                    // Parse deux valeurs: (val1, val2)
                    String[] values = listValue.split("\\s*,\\s*");
                    if (values.length != 2) {
                        throw new IllegalArgumentException("BETWEEN nécessite exactement 2 valeurs: " + condition);
                    }
                    java.util.List<Object> valueList = new java.util.ArrayList<>();
                    for (String val : values) {
                        val = val.trim();
                        if (val.startsWith("'") && val.endsWith("'")) {
                            valueList.add(val.substring(1, val.length() - 1));
                        } else {
                            try {
                                valueList.add(Integer.parseInt(val));
                            } catch (NumberFormatException e) {
                                valueList.add(Double.parseDouble(val));
                            }
                        }
                    }
                    return valueList;
                } else {
                    throw new IllegalArgumentException("Valeurs manquantes pour BETWEEN: " + condition);
                }
                
            default:
                // Opérateurs standards
                if (stringValue != null) {
                    return stringValue;
                } else if (numericValue != null) {
                    try {
                        return Integer.parseInt(numericValue);
                    } catch (NumberFormatException e) {
                        return Double.parseDouble(numericValue);
                    }
                } else {
                    throw new IllegalArgumentException("Valeur manquante pour la condition: " + condition);
                }
        }
    }
} 