package com.memorydb.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Représente une table dans la base de données
 */
public class Table {
    private final String name;
    private final List<Column> columns;
    private final Map<String, Integer> columnIndexMap;
    private final ReadWriteLock lock;
    
    /**
     * Crée une nouvelle table
     * @param name Le nom de la table
     * @param columns La liste des colonnes
     */
    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.columnIndexMap = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        
        // Construit la map des index de colonnes
        for (int i = 0; i < columns.size(); i++) {
            columnIndexMap.put(columns.get(i).getName(), i);
        }
    }
    
    /**
     * Obtient le nom de la table
     * @return Le nom de la table
     */
    public String getName() {
        return name;
    }
    
    /**
     * Obtient la liste des colonnes
     * @return La liste des colonnes
     */
    public List<Column> getColumns() {
        return new ArrayList<>(columns);
    }
    
    /**
     * Obtient l'index d'une colonne par son nom
     * @param columnName Le nom de la colonne
     * @return L'index de la colonne, ou -1 si elle n'existe pas
     */
    public int getColumnIndex(String columnName) {
        Integer index = columnIndexMap.get(columnName);
        return index != null ? index : -1;
    }
    
    /**
     * Obtient une colonne par son nom
     * @param columnName Le nom de la colonne
     * @return La colonne, ou null si elle n'existe pas
     */
    public Column getColumn(String columnName) {
        int index = getColumnIndex(columnName);
        if (index >= 0) {
            return columns.get(index);
        }
        return null;
    }
    
    /**
     * Acquiert un verrou en lecture
     */
    public void readLock() {
        lock.readLock().lock();
    }
    
    /**
     * Libère le verrou en lecture
     */
    public void readUnlock() {
        lock.readLock().unlock();
    }
    
    /**
     * Acquiert un verrou en écriture
     */
    public void writeLock() {
        lock.writeLock().lock();
    }
    
    /**
     * Libère le verrou en écriture
     */
    public void writeUnlock() {
        lock.writeLock().unlock();
    }
    
    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                '}';
    }
} 