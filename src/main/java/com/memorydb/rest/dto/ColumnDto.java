package com.memorydb.rest.dto;

/**
 * DTO pour une colonne
 */
public class ColumnDto {
    private String name;
    private String type;
    private boolean nullable;
    
    public ColumnDto() {
        // Constructeur par défaut pour la désérialisation
    }
    
    public ColumnDto(String name, String type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public boolean isNullable() {
        return nullable;
    }
    
    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }
} 