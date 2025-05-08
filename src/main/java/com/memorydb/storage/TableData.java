package com.memorydb.storage;

import com.memorydb.core.Column;
import com.memorydb.core.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Stockage des données d'une table
 */
public class TableData {
    private final Table table;
    private final List<ColumnStore> columnStores;
    private final ReadWriteLock lock;
    private int rowCount;
    
    /**
     * Crée un nouveau stockage pour une table
     * @param table La table
     */
    public TableData(Table table) {
        this.table = table;
        this.columnStores = new ArrayList<>(table.getColumns().size());
        this.lock = new ReentrantReadWriteLock();
        this.rowCount = 0;
        
        // Crée un ColumnStore pour chaque colonne
        for (Column column : table.getColumns()) {
            columnStores.add(new ColumnStore(column));
        }
    }
    
    /**
     * Obtient la table
     * @return La table
     */
    public Table getTable() {
        return table;
    }
    
    /**
     * Obtient le nombre de lignes
     * @return Le nombre de lignes
     */
    public int getRowCount() {
        lock.readLock().lock();
        try {
            return rowCount;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Obtient un ColumnStore pour une colonne donnée
     * @param columnIndex L'index de la colonne
     * @return Le ColumnStore
     */
    public ColumnStore getColumnStore(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnStores.size()) {
            throw new IndexOutOfBoundsException("Indice de colonne invalide: " + columnIndex);
        }
        return columnStores.get(columnIndex);
    }
    
    /**
     * Obtient un ColumnStore pour une colonne donnée
     * @param columnName Le nom de la colonne
     * @return Le ColumnStore
     */
    public ColumnStore getColumnStore(String columnName) {
        int columnIndex = table.getColumnIndex(columnName);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Colonne inconnue: " + columnName);
        }
        return getColumnStore(columnIndex);
    }
    
    /**
     * Ajoute une nouvelle ligne à la table
     * @param values Les valeurs pour chaque colonne
     */
    public void addRow(Object[] values) {
        if (values.length != columnStores.size()) {
            throw new IllegalArgumentException("Nombre de valeurs incorrect, attendu: " + 
                columnStores.size() + ", obtenu: " + values.length);
        }
        
        lock.writeLock().lock();
        try {
            for (int i = 0; i < values.length; i++) {
                addValue(i, values[i]);
            }
            rowCount++;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Ajoute une valeur à une colonne
     * @param columnIndex L'index de la colonne
     * @param value La valeur à ajouter
     */
    private void addValue(int columnIndex, Object value) {
        ColumnStore columnStore = columnStores.get(columnIndex);
        
        if (value == null) {
            columnStore.addNull();
            return;
        }
        
        switch (columnStore.getType()) {
            case INTEGER:
                columnStore.addInt((Integer) value);
                break;
            case LONG:
                columnStore.addLong((Long) value);
                break;
            case FLOAT:
                columnStore.addFloat((Float) value);
                break;
            case DOUBLE:
                columnStore.addDouble((Double) value);
                break;
            case BOOLEAN:
                columnStore.addBoolean((Boolean) value);
                break;
            case STRING:
                columnStore.addString((String) value);
                break;
            case DATE:
            case TIMESTAMP:
                columnStore.addDate((Long) value);
                break;
            default:
                throw new IllegalArgumentException("Type non supporté: " + columnStore.getType());
        }
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
    
    /**
     * Incrémente le compteur de lignes
     * Utilisé par les loaders optimisés pour éviter des allocations inutiles
     * ATTENTION: Cette méthode doit être appelée uniquement après avoir ajouté toutes les valeurs de colonne
     */
    public void incrementRowCount() {
        lock.writeLock().lock();
        try {
            rowCount++;
        } finally {
            lock.writeLock().unlock();
        }
    }
} 