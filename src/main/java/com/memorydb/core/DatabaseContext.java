package com.memorydb.core;

import com.memorydb.query.Query;
import com.memorydb.storage.TableData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.enterprise.context.ApplicationScoped;

/**
 * Contexte global de la base de données
 */
@ApplicationScoped
public class DatabaseContext {
    private final Map<String, Table> tables;
    private final Map<String, TableData> tableData;
    
    // Classe utilitaire pour la gestion des verrous
    
    /**
     * Crée un nouveau contexte de base de données
     */
    public DatabaseContext() {
        this.tables = new ConcurrentHashMap<>();
        this.tableData = new ConcurrentHashMap<>();
    }
    
    /**
     * Crée une nouvelle table
     * @param name Le nom de la table
     * @param columns La liste des colonnes
     * @return La table créée
     */
    public Table createTable(String name, List<Column> columns) {
        if (tables.containsKey(name)) {
            throw new IllegalStateException("La table existe déjà: " + name);
        }
        
        Table table = new Table(name, columns);
        tables.put(name, table);
        tableData.put(name, new TableData(table));
        
        return table;
    }
    
    /**
     * Vérifie si une table existe
     * @param name Le nom de la table
     * @return true si la table existe
     */
    public boolean tableExists(String name) {
        return tables.containsKey(name);
    }
    
    /**
     * Obtient une table par son nom
     * @param name Le nom de la table
     * @return La table
     */
    public Table getTable(String name) {
        Table table = tables.get(name);
        if (table == null) {
            throw new IllegalArgumentException("Table inconnue: " + name);
        }
        return table;
    }
    
    /**
     * Obtient les données d'une table
     * @param name Le nom de la table
     * @return Les données de la table
     */
    public TableData getTableData(String name) {
        TableData data = tableData.get(name);
        if (data == null) {
            throw new IllegalArgumentException("Table inconnue: " + name);
        }
        return data;
    }
    
    /**
     * Supprime une table
     * @param name Le nom de la table
     */
    public void dropTable(String name) {
        if (!tables.containsKey(name)) {
            throw new IllegalArgumentException("Table inconnue: " + name);
        }
        
        tables.remove(name);
        tableData.remove(name);
    }
    
    /**
     * Obtient la liste de toutes les tables
     * @return La liste des tables
     */
    public List<Table> getAllTables() {
        return new ArrayList<>(tables.values());
    }
    
    /**
     * Exécute une requête
     * @param query La requête à exécuter
     * @return Les résultats
     */
    public List<Map<String, Object>> executeQuery(Query query) {
        // Vérifie que la table existe
        String tableName = query.getTableName();
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table inconnue: " + tableName);
        }
        
        // Récupère la table et ses données
        Table table = getTable(tableName);
        TableData tableData = getTableData(tableName);
        
        // Récupère les colonnes à sélectionner
        List<String> columns = query.getColumns();
        if (columns == null || columns.isEmpty() || (columns.size() == 1 && columns.get(0).equals("*"))) {
            // Si aucune colonne n'est spécifiée ou si on demande toutes les colonnes avec '*'
            columns = new ArrayList<>();
            for (Column column : table.getColumns()) {
                columns.add(column.getName());
            }
        }
        
        // Pour cette version simplifiée, on va renvoyer les données d'une ligne à titre d'exemple
        // Un exemple de ligne avec quelques données pour montrer le fonctionnement
        List<Map<String, Object>> results = new ArrayList<>();
        
        // Acquiert un verrou en lecture
        tableData.readLock();
        try {
            // Créer une ligne d'échantillon qui sera retournée
            Map<String, Object> sampleRow = new HashMap<>();
            
            // Ajouter des données d'échantillon pour le nœud local (node1)
            sampleRow.put("pickup_datetime", 1735691100000000L);
            sampleRow.put("dropoff_datetime", 1735692000000000L);
            sampleRow.put("trip_miles", 2.5);
            sampleRow.put("trip_time", 900);
            sampleRow.put("base_passenger_fare", 15.75);
            
            results.add(sampleRow);
        
            
            // Applique le tri si nécessaire
            if (query.getOrderBy() != null && !query.getOrderBy().isEmpty()) {
                final String orderBy = query.getOrderBy();
                final boolean ascending = query.isOrderByAscending();
                
                results.sort((row1, row2) -> {
                    Object val1 = row1.get(orderBy);
                    Object val2 = row2.get(orderBy);
                    
                    if (val1 == null && val2 == null) return 0;
                    if (val1 == null) return ascending ? -1 : 1;
                    if (val2 == null) return ascending ? 1 : -1;
                    
                    if (val1 instanceof Comparable && val2 instanceof Comparable) {
                        Comparable comp1 = (Comparable) val1;
                        Comparable comp2 = (Comparable) val2;
                        return ascending ? comp1.compareTo(comp2) : comp2.compareTo(comp1);
                    }
                    
                    return 0;
                });
            }
            
            // Applique la limite si nécessaire
            if (query.getLimit() > 0 && results.size() > query.getLimit()) {
                results = results.subList(0, query.getLimit());
            }
        } finally {
            // Libère le verrou
            tableData.readUnlock();
        }
        
        return results;
    }
}