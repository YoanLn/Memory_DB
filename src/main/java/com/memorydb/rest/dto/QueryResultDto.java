package com.memorydb.rest.dto;

import java.util.List;
import java.util.Map;

/**
 * DTO pour le résultat d'une requête
 */
public class QueryResultDto {
    private List<String> columns;
    private List<Map<String, Object>> rows;
    private int rowCount;
    
    public QueryResultDto() {
        // Constructeur par défaut pour la sérialisation
    }
    
    public QueryResultDto(List<String> columns, List<Map<String, Object>> rows) {
        this.columns = columns;
        this.rows = rows;
        this.rowCount = rows.size();
    }
    
    public List<String> getColumns() {
        return columns;
    }
    
    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
    
    public List<Map<String, Object>> getRows() {
        return rows;
    }
    
    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
        this.rowCount = rows.size();
    }
    
    public int getRowCount() {
        return rowCount;
    }
    
    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }
} 