package com.memorydb.query;

/**
 * Types d'opérateurs de conditions supportés
 */
public enum ConditionType {
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_OR_EQUALS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUALS,
    LIKE,
    IS_NULL,
    IS_NOT_NULL
} 