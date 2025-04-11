package com.memorydb.storage;

import com.memorydb.common.DataType;
import com.memorydb.core.Column;

/**
 * Stockage en colonnes pour les données en mémoire
 */
public class ColumnStore {
    private final Column column;
    private final DataType type;
    private int capacity;
    private int size;
    
    // Stockage typé pour chaque type de données
    private int[] intValues;
    private long[] longValues;
    private float[] floatValues;
    private double[] doubleValues;
    private boolean[] booleanValues;
    private String[] stringValues;
    private long[] dateValues; // stockées en millisecondes depuis l'epoch
    private boolean[] nullFlags; // indique si la valeur est null
    
    private static final int INITIAL_CAPACITY = 1024;
    private static final float GROWTH_FACTOR = 1.5f;
    
    /**
     * Crée un nouveau stockage en colonnes
     * @param column La définition de la colonne
     */
    public ColumnStore(Column column) {
        this.column = column;
        this.type = column.getType();
        this.capacity = INITIAL_CAPACITY;
        this.size = 0;
        
        // Initialise le stockage approprié selon le type
        switch (type) {
            case INTEGER:
                intValues = new int[capacity];
                break;
            case LONG:
            case TIMESTAMP:
                longValues = new long[capacity];
                break;
            case FLOAT:
                floatValues = new float[capacity];
                break;
            case DOUBLE:
                doubleValues = new double[capacity];
                break;
            case BOOLEAN:
                booleanValues = new boolean[capacity];
                break;
            case STRING:
                stringValues = new String[capacity];
                break;
            case DATE:
                dateValues = new long[capacity];
                break;
        }
        
        // Crée le tableau des flags null si la colonne est nullable
        if (column.isNullable()) {
            nullFlags = new boolean[capacity];
        }
    }
    
    /**
     * Ajoute une valeur entière à la colonne
     * @param value La valeur à ajouter
     */
    public void addInt(int value) {
        ensureCapacity(size + 1);
        intValues[size++] = value;
    }
    
    /**
     * Ajoute une valeur longue à la colonne
     * @param value La valeur à ajouter
     */
    public void addLong(long value) {
        ensureCapacity(size + 1);
        longValues[size++] = value;
    }
    
    /**
     * Ajoute une valeur float à la colonne
     * @param value La valeur à ajouter
     */
    public void addFloat(float value) {
        ensureCapacity(size + 1);
        floatValues[size++] = value;
    }
    
    /**
     * Ajoute une valeur double à la colonne
     * @param value La valeur à ajouter
     */
    public void addDouble(double value) {
        ensureCapacity(size + 1);
        doubleValues[size++] = value;
    }
    
    /**
     * Ajoute une valeur booléenne à la colonne
     * @param value La valeur à ajouter
     */
    public void addBoolean(boolean value) {
        ensureCapacity(size + 1);
        booleanValues[size++] = value;
    }
    
    /**
     * Ajoute une chaîne à la colonne
     * @param value La valeur à ajouter
     */
    public void addString(String value) {
        ensureCapacity(size + 1);
        stringValues[size++] = value;
    }
    
    /**
     * Ajoute une valeur de date à la colonne (stockée en millisecondes)
     * @param value La valeur à ajouter (millisecondes depuis l'epoch)
     */
    public void addDate(long value) {
        ensureCapacity(size + 1);
        dateValues[size++] = value;
    }
    
    /**
     * Ajoute une valeur null à la colonne
     */
    public void addNull() {
        if (!column.isNullable()) {
            throw new IllegalStateException("La colonne n'est pas nullable");
        }
        ensureCapacity(size + 1);
        nullFlags[size] = true;
        size++;
    }
    
    /**
     * Obtient une valeur entière de la colonne
     * @param index L'index de la valeur
     * @return La valeur entière
     */
    public int getInt(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        if (nullFlags != null && nullFlags[index]) {
            throw new NullPointerException("La valeur à l'index " + index + " est null");
        }
        return intValues[index];
    }
    
    /**
     * Obtient une valeur longue de la colonne
     * @param index L'index de la valeur
     * @return La valeur longue
     */
    public long getLong(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        if (nullFlags != null && nullFlags[index]) {
            throw new NullPointerException("La valeur à l'index " + index + " est null");
        }
        return longValues[index];
    }
    
    /**
     * Obtient une valeur float de la colonne
     * @param index L'index de la valeur
     * @return La valeur float
     */
    public float getFloat(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        if (nullFlags != null && nullFlags[index]) {
            throw new NullPointerException("La valeur à l'index " + index + " est null");
        }
        return floatValues[index];
    }
    
    /**
     * Obtient une valeur double de la colonne
     * @param index L'index de la valeur
     * @return La valeur double
     */
    public double getDouble(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        if (nullFlags != null && nullFlags[index]) {
            throw new NullPointerException("La valeur à l'index " + index + " est null");
        }
        return doubleValues[index];
    }
    
    /**
     * Obtient une valeur booléenne de la colonne
     * @param index L'index de la valeur
     * @return La valeur booléenne
     */
    public boolean getBoolean(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        if (nullFlags != null && nullFlags[index]) {
            throw new NullPointerException("La valeur à l'index " + index + " est null");
        }
        return booleanValues[index];
    }
    
    /**
     * Obtient une chaîne de la colonne
     * @param index L'index de la valeur
     * @return La chaîne
     */
    public String getString(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        if (nullFlags != null && nullFlags[index]) {
            return null;
        }
        return stringValues[index];
    }
    
    /**
     * Obtient une valeur de date de la colonne
     * @param index L'index de la valeur
     * @return La valeur de date (millisecondes depuis l'epoch)
     */
    public long getDate(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        if (nullFlags != null && nullFlags[index]) {
            throw new NullPointerException("La valeur à l'index " + index + " est null");
        }
        return dateValues[index];
    }
    
    /**
     * Vérifie si une valeur est null
     * @param index L'index de la valeur
     * @return true si la valeur est null, false sinon
     */
    public boolean isNull(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        return nullFlags != null && nullFlags[index];
    }
    
    /**
     * Obtient le type de données de la colonne
     * @return Le type de données
     */
    public DataType getType() {
        return type;
    }
    
    /**
     * Obtient la taille de la colonne (nombre de valeurs)
     * @return La taille
     */
    public int size() {
        return size;
    }
    
    /**
     * Obtient la définition de la colonne
     * @return La définition de la colonne
     */
    public Column getColumn() {
        return column;
    }
    
    /**
     * S'assure que la capacité est suffisante pour stocker plus de valeurs
     * @param minCapacity La capacité minimale requise
     */
    private void ensureCapacity(int minCapacity) {
        if (minCapacity > capacity) {
            int newCapacity = Math.max(minCapacity, (int) (capacity * GROWTH_FACTOR));
            
            switch (type) {
                case INTEGER:
                    int[] newIntValues = new int[newCapacity];
                    System.arraycopy(intValues, 0, newIntValues, 0, size);
                    intValues = newIntValues;
                    break;
                case LONG:
                case TIMESTAMP:
                    long[] newLongValues = new long[newCapacity];
                    System.arraycopy(longValues, 0, newLongValues, 0, size);
                    longValues = newLongValues;
                    break;
                case FLOAT:
                    float[] newFloatValues = new float[newCapacity];
                    System.arraycopy(floatValues, 0, newFloatValues, 0, size);
                    floatValues = newFloatValues;
                    break;
                case DOUBLE:
                    double[] newDoubleValues = new double[newCapacity];
                    System.arraycopy(doubleValues, 0, newDoubleValues, 0, size);
                    doubleValues = newDoubleValues;
                    break;
                case BOOLEAN:
                    boolean[] newBooleanValues = new boolean[newCapacity];
                    System.arraycopy(booleanValues, 0, newBooleanValues, 0, size);
                    booleanValues = newBooleanValues;
                    break;
                case STRING:
                    String[] newStringValues = new String[newCapacity];
                    System.arraycopy(stringValues, 0, newStringValues, 0, size);
                    stringValues = newStringValues;
                    break;
                case DATE:
                    long[] newDateValues = new long[newCapacity];
                    System.arraycopy(dateValues, 0, newDateValues, 0, size);
                    dateValues = newDateValues;
                    break;
            }
            
            if (nullFlags != null) {
                boolean[] newNullFlags = new boolean[newCapacity];
                System.arraycopy(nullFlags, 0, newNullFlags, 0, size);
                nullFlags = newNullFlags;
            }
            
            capacity = newCapacity;
        }
    }
} 