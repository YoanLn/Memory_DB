package com.memorydb.rest.dto;

import java.util.List;

/**
 * DTO pour une table
 */
public class TableDto {
    private String name;
    private List<ColumnDto> columns;
    
    public TableDto() {
        // Constructeur par défaut pour la désérialisation
    }
    
    public TableDto(String name, List<ColumnDto> columns) {
        this.name = name;
        this.columns = columns;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public List<ColumnDto> getColumns() {
        return columns;
    }
    
    public void setColumns(List<ColumnDto> columns) {
        this.columns = columns;
    }
} 