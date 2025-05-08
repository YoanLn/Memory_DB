package com.memorydb.storage;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import com.memorydb.common.DataType;
import com.memorydb.core.Column;
import com.memorydb.index.BitmapIndex;

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
    private long[] dateValues; // stockées en millisecondes depuis l'epoch
    private boolean[] nullFlags; // indique si la valeur est null
    
    // Optimisation pour les colonnes de type String
    private Map<String, Integer> stringDictionary; // dictionnaire pour la compression des chaînes
    private int[] stringDictionaryIndex; // index dans le dictionnaire pour chaque valeur
    private String[] dictionaryValues; // tableau inverse pour retrouver la chaîne à partir de l'index
    private int dictionarySize; // nombre de chaînes uniques dans le dictionnaire
    
    // Bitmap indexes pour accélérer les requêtes
    private BitmapIndex<Integer> intIndex;    // Pour les INTEGER
    private BitmapIndex<Long> longIndex;      // Pour les LONG, DATE, TIMESTAMP
    private BitmapIndex<Float> floatIndex;    // Pour les FLOAT
    private BitmapIndex<Double> doubleIndex;  // Pour les DOUBLE
    private BitmapIndex<Boolean> boolIndex;   // Pour les BOOLEAN
    private BitmapIndex<String> stringIndex;  // Pour les STRING
    
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
        
        // Initialiser les index bitmap si nécessaire
        // Nous créons uniquement les index pour les colonnes susceptibles d'être utilisées dans des conditions de filtre
        if (column.isIndexed()) {
            initializeBitmapIndex();
        }
        
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
                // Utilisation d'un dictionnaire pour optimiser le stockage des chaînes
                stringDictionary = new HashMap<>();
                stringDictionaryIndex = new int[capacity];
                dictionaryValues = new String[1024]; // Taille initiale du dictionnaire
                dictionarySize = 0;
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
        intValues[size] = value;
        
        // Mettre à jour l'index bitmap si activé
        if (intIndex != null && intIndex.isEnabled()) {
            intIndex.add(value, size);
        }
        
        size++;
    }
    
    /**
     * Ajoute une valeur longue à la colonne
     * @param value La valeur à ajouter
     */
    public void addLong(long value) {
        ensureCapacity(size + 1);
        longValues[size] = value;
        
        // Mettre à jour l'index bitmap si activé
        if (longIndex != null && longIndex.isEnabled()) {
            longIndex.add(value, size);
        }
        
        size++;
    }
    
    /**
     * Ajoute une valeur float à la colonne
     * @param value La valeur à ajouter
     */
    public void addFloat(float value) {
        ensureCapacity(size + 1);
        floatValues[size] = value;
        
        // Mettre à jour l'index bitmap si activé
        if (floatIndex != null && floatIndex.isEnabled()) {
            floatIndex.add(value, size);
        }
        
        size++;
    }
    
    /**
     * Ajoute une valeur double à la colonne
     * @param value La valeur à ajouter
     */
    public void addDouble(double value) {
        ensureCapacity(size + 1);
        doubleValues[size] = value;
        
        // Mettre à jour l'index bitmap si activé
        if (doubleIndex != null && doubleIndex.isEnabled()) {
            doubleIndex.add(value, size);
        }
        
        size++;
    }
    
    /**
     * Ajoute une valeur booléenne à la colonne
     * @param value La valeur à ajouter
     */
    public void addBoolean(boolean value) {
        ensureCapacity(size + 1);
        booleanValues[size] = value;
        
        // Mettre à jour l'index bitmap si activé
        if (boolIndex != null && boolIndex.isEnabled()) {
            boolIndex.add(value, size);
        }
        
        size++;
    }
    
    /**
     * Ajoute une chaîne à la colonne avec compression par dictionnaire
     * @param value La valeur à ajouter
     */
    public void addString(String value) {
        ensureCapacity(size + 1);
        
        // Traiter les valeurs null spécialement
        if (value == null) {
            addNull();
            return;
        }
        
        // Réutiliser les chaînes identiques via l'interning pour réduire la consommation mémoire
        value = value.intern();
        
        // Compression par dictionnaire
        Integer dictIndex = stringDictionary.get(value);
        if (dictIndex == null) {
            // Nouvelle chaîne, ajouter au dictionnaire
            dictIndex = dictionarySize;
            stringDictionary.put(value, dictIndex);
            
            // S'assurer que le tableau du dictionnaire a suffisamment de place
            if (dictionarySize >= dictionaryValues.length) {
                int newCapacity = Math.max(dictionarySize + 1, (int)(dictionaryValues.length * 1.5f));
                String[] newDict = new String[newCapacity];
                System.arraycopy(dictionaryValues, 0, newDict, 0, dictionarySize);
                dictionaryValues = newDict;
            }
            
            dictionaryValues[dictionarySize++] = value;
        }
        
        stringDictionaryIndex[size] = dictIndex;
        
        // Mettre à jour l'index bitmap si activé
        if (stringIndex != null && stringIndex.isEnabled()) {
            stringIndex.add(value, size);
        }
        
        size++;
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
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Indice invalide: " + index);
        }
        
        if (nullFlags != null && nullFlags[index]) {
            return null;
        }
        
        int dictIndex = stringDictionaryIndex[index];
        return dictionaryValues[dictIndex];
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
     * Initialise les index bitmap en fonction du type de données
     */
    private void initializeBitmapIndex() {
        switch (type) {
            case INTEGER:
                intIndex = new BitmapIndex<>();
                break;
            case LONG:
            case TIMESTAMP:
            case DATE:
                longIndex = new BitmapIndex<>();
                break;
            case FLOAT:
                floatIndex = new BitmapIndex<>();
                break;
            case DOUBLE:
                doubleIndex = new BitmapIndex<>();
                break;
            case BOOLEAN:
                boolIndex = new BitmapIndex<>();
                break;
            case STRING:
                stringIndex = new BitmapIndex<>();
                break;
        }
    }
    
    /**
     * Recherche rapide des lignes qui correspondent à une valeur égale à la valeur spécifiée
     * @param value La valeur à rechercher
     * @return BitSet contenant les indices des lignes correspondantes, ou null si l'index n'est pas disponible
     */
    public BitSet findEqual(Object value) {
        if (value == null) {
            return null; // Cas spécial pour null, à implémenter si nécessaire
        }
        
        switch (type) {
            case INTEGER:
                if (intIndex != null) {
                    return intIndex.search((Integer) value);
                }
                break;
            case LONG:
            case TIMESTAMP:
            case DATE:
                if (longIndex != null) {
                    return longIndex.search((Long) value);
                }
                break;
            case FLOAT:
                if (floatIndex != null) {
                    return floatIndex.search((Float) value);
                }
                break;
            case DOUBLE:
                if (doubleIndex != null) {
                    return doubleIndex.search((Double) value);
                }
                break;
            case BOOLEAN:
                if (boolIndex != null) {
                    return boolIndex.search((Boolean) value);
                }
                break;
            case STRING:
                if (stringIndex != null) {
                    return stringIndex.search((String) value);
                }
                break;
        }
        
        return null; // Index non disponible
    }
    
    /**
     * Réinitialise les index bitmap
     * Utile après des modifications massives dans la table
     */
    public void resetIndexes() {
        if (intIndex != null) intIndex.clear();
        if (longIndex != null) longIndex.clear();
        if (floatIndex != null) floatIndex.clear();
        if (doubleIndex != null) doubleIndex.clear();
        if (boolIndex != null) boolIndex.clear();
        if (stringIndex != null) stringIndex.clear();
        
        // Reconstruit les index si nécessaire
        if (column.isIndexed()) {
            rebuildIndexes();
        }
    }
    
    /**
     * Reconstruit tous les index bitmap
     */
    private void rebuildIndexes() {
        // Rien à faire si la colonne n'est pas indexée
        if (!column.isIndexed()) return;
        
        switch (type) {
            case INTEGER:
                intIndex = new BitmapIndex<>();
                for (int i = 0; i < size; i++) {
                    if (nullFlags == null || !nullFlags[i]) {
                        intIndex.add(intValues[i], i);
                    }
                }
                break;
            case LONG:
            case TIMESTAMP:
            case DATE:
                longIndex = new BitmapIndex<>();
                for (int i = 0; i < size; i++) {
                    if (nullFlags == null || !nullFlags[i]) {
                        longIndex.add(longValues[i], i);
                    }
                }
                break;
            case FLOAT:
                floatIndex = new BitmapIndex<>();
                for (int i = 0; i < size; i++) {
                    if (nullFlags == null || !nullFlags[i]) {
                        floatIndex.add(floatValues[i], i);
                    }
                }
                break;
            case DOUBLE:
                doubleIndex = new BitmapIndex<>();
                for (int i = 0; i < size; i++) {
                    if (nullFlags == null || !nullFlags[i]) {
                        doubleIndex.add(doubleValues[i], i);
                    }
                }
                break;
            case BOOLEAN:
                boolIndex = new BitmapIndex<>();
                for (int i = 0; i < size; i++) {
                    if (nullFlags == null || !nullFlags[i]) {
                        boolIndex.add(booleanValues[i], i);
                    }
                }
                break;
            case STRING:
                stringIndex = new BitmapIndex<>();
                for (int i = 0; i < size; i++) {
                    if (nullFlags == null || !nullFlags[i]) {
                        int dictIndex = stringDictionaryIndex[i];
                        stringIndex.add(dictionaryValues[dictIndex], i);
                    }
                }
                break;
        }
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
                    // Redimensionner uniquement le tableau des index du dictionnaire
                    int[] newStringDictionaryIndex = new int[newCapacity];
                    System.arraycopy(stringDictionaryIndex, 0, newStringDictionaryIndex, 0, size);
                    stringDictionaryIndex = newStringDictionaryIndex;
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