package com.memorydb.rest.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DTO pour le résultat d'une requête
 * Optimisé pour réduire l'empreinte mémoire avec stockage en tableaux
 */
public class QueryResultDto {
    private List<String> columns;
    
    // Stockage optimisé en mémoire: tableau bidimensionnel au lieu de List<Map>
    private Object[][] data;
    private int rowCount;
    
    // Cache transient pour la sérialisation JSON
    private transient List<Map<String, Object>> rowsCache;
    
    public QueryResultDto() {
        // Constructeur par défaut pour la sérialisation
    }
    
    /**
     * Constructeur optimisé à partir des tableaux d'objets directement
     */
    public QueryResultDto(List<String> columns, Object[][] data) {
        this.columns = columns;
        this.data = data;
        this.rowCount = data != null ? data.length : 0;
    }
    
    /**
     * Constructeur de compatibilité à partir de List<Map>
     */
    public QueryResultDto(List<String> columns, List<Map<String, Object>> rows) {
        this.columns = columns;
        this.rowCount = rows.size();
        
        // Conversion vers le format de stockage optimisé
        convertToArrayStorage(rows);
    }
    
    private void convertToArrayStorage(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            this.data = new Object[0][0];
            return;
        }
        
        this.data = new Object[rows.size()][columns.size()];
        
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            for (int j = 0; j < columns.size(); j++) {
                this.data[i][j] = row.get(columns.get(j));
            }
        }
    }
    
    public List<String> getColumns() {
        return columns;
    }
    
    public void setColumns(List<String> columns) {
        this.columns = columns;
        // Invalider le cache si les colonnes changent
        this.rowsCache = null;
    }
    
    /**
     * Méthode de compatibilité pour obtenir les lignes au format Map
     * pour la sérialisation JSON
     */
    public List<Map<String, Object>> getRows() {
        if (rowsCache != null) {
            return rowsCache;
        }
        
        if (data == null || columns == null) {
            return Collections.emptyList();
        }
        
        rowsCache = new ArrayList<>(data.length);
        for (Object[] row : data) {
            Map<String, Object> rowMap = new HashMap<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                if (i < row.length) { // Éviter les IndexOutOfBoundsException
                    rowMap.put(columns.get(i), row[i]);
                }
            }
            rowsCache.add(rowMap);
        }
        
        return rowsCache;
    }
    
    /**
     * Méthode de compatibilité pour définir les lignes à partir de Maps
     */
    public void setRows(List<Map<String, Object>> rows) {
        convertToArrayStorage(rows);
        this.rowCount = rows != null ? rows.size() : 0;
        this.rowsCache = null;
    }
    
    /**
     * Accès direct aux données brutes pour un traitement plus efficace
     */
    public Object[][] getRawData() {
        return data;
    }
    
    /**
     * Définir les données directement sous forme de tableau bidimensionnel
     */
    public void setRawData(Object[][] data) {
        this.data = data;
        this.rowCount = data != null ? data.length : 0;
        this.rowsCache = null;
    }
    
    /**
     * Obtenir une valeur spécifique sans créer de Map
     */
    public Object getValueAt(int row, int column) {
        if (data == null || row >= data.length || column >= data[row].length) {
            return null;
        }
        return data[row][column];
    }
    
    /**
     * Obtenir une valeur spécifique par nom de colonne
     */
    public Object getValueAt(int row, String columnName) {
        if (data == null || columns == null) {
            return null;
        }
        int columnIndex = columns.indexOf(columnName);
        if (columnIndex == -1 || row >= data.length || columnIndex >= data[row].length) {
            return null;
        }
        return data[row][columnIndex];
    }
    
    public int getRowCount() {
        return rowCount;
    }
    
    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }
} 