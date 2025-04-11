package com.memorydb.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Résultat d'une requête
 */
public class QueryResult {
    private final List<String> columns;
    private final List<Map<String, Object>> rows;
    
    /**
     * Crée un nouveau résultat de requête
     * @param columns Les colonnes du résultat
     * @param rows Les lignes du résultat
     */
    public QueryResult(List<String> columns, List<Map<String, Object>> rows) {
        this.columns = new ArrayList<>(columns);
        this.rows = new ArrayList<>(rows);
    }
    
    /**
     * Obtient les colonnes du résultat
     * @return Les colonnes
     */
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }
    
    /**
     * Obtient les lignes du résultat
     * @return Les lignes
     */
    public List<Map<String, Object>> getRows() {
        return new ArrayList<>(rows);
    }
    
    /**
     * Obtient le nombre de lignes
     * @return Le nombre de lignes
     */
    public int getRowCount() {
        return rows.size();
    }
    
    /**
     * Obtient le nombre de colonnes
     * @return Le nombre de colonnes
     */
    public int getColumnCount() {
        return columns.size();
    }
    
    /**
     * Obtient une valeur dans le résultat
     * @param rowIndex L'index de la ligne
     * @param columnName Le nom de la colonne
     * @return La valeur
     */
    public Object getValue(int rowIndex, String columnName) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            throw new IndexOutOfBoundsException("Indice de ligne invalide: " + rowIndex);
        }
        
        Map<String, Object> row = rows.get(rowIndex);
        if (!row.containsKey(columnName)) {
            throw new IllegalArgumentException("Colonne inconnue: " + columnName);
        }
        
        return row.get(columnName);
    }
    
    /**
     * Affiche le résultat de la requête
     */
    @Override
    public String toString() {
        if (columns.isEmpty() || rows.isEmpty()) {
            return "Résultat vide";
        }
        
        StringBuilder sb = new StringBuilder();
        
        // Calcule la largeur de chaque colonne
        int[] columnWidths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            columnWidths[i] = columnName.length();
            
            for (Map<String, Object> row : rows) {
                Object value = row.get(columnName);
                int valueLength = value == null ? 4 : value.toString().length();
                columnWidths[i] = Math.max(columnWidths[i], valueLength);
            }
        }
        
        // Affiche l'en-tête
        for (int i = 0; i < columns.size(); i++) {
            sb.append(String.format("%-" + (columnWidths[i] + 2) + "s", columns.get(i)));
        }
        sb.append("\n");
        
        // Affiche une ligne de séparation
        for (int width : columnWidths) {
            for (int i = 0; i < width + 2; i++) {
                sb.append("-");
            }
        }
        sb.append("\n");
        
        // Affiche les lignes
        for (Map<String, Object> row : rows) {
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                Object value = row.get(columnName);
                String valueStr = value == null ? "NULL" : value.toString();
                sb.append(String.format("%-" + (columnWidths[i] + 2) + "s", valueStr));
            }
            sb.append("\n");
        }
        
        return sb.toString();
    }
} 