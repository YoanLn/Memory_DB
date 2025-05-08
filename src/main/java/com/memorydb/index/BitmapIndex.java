package com.memorydb.index;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Implémentation d'un index bitmap pour accélérer les requêtes
 * avec conditions d'égalité sur les colonnes
 */
public class BitmapIndex<T> {
    private final Map<T, BitSet> bitmaps;
    private boolean isEnabled;
    
    /**
     * Crée un nouvel index bitmap
     */
    public BitmapIndex() {
        this.bitmaps = new HashMap<>();
        this.isEnabled = true;
    }
    
    /**
     * Active ou désactive l'index
     * @param enabled true pour activer, false pour désactiver
     */
    public void setEnabled(boolean enabled) {
        this.isEnabled = enabled;
    }
    
    /**
     * Vérifie si l'index est activé
     * @return true si l'index est activé
     */
    public boolean isEnabled() {
        return isEnabled;
    }
    
    /**
     * Ajoute une valeur à l'index
     * @param value La valeur à indexer
     * @param rowId L'identifiant de la ligne
     */
    public void add(T value, int rowId) {
        if (!isEnabled) return;
        
        if (value == null) {
            // Traitement spécial pour les valeurs nulles
            getOrCreateBitmap(null).set(rowId);
            return;
        }
        
        // Ajoute le rowId au bitmap correspondant à la valeur
        getOrCreateBitmap(value).set(rowId);
    }
    
    /**
     * Recherche les lignes correspondant à une valeur
     * @param value La valeur recherchée
     * @return Un bitset avec les identifiants des lignes correspondantes
     */
    public BitSet search(T value) {
        BitSet result = bitmaps.get(value);
        return result != null ? (BitSet) result.clone() : new BitSet();
    }
    
    /**
     * Recherche les lignes qui ne correspondent pas à une valeur
     * @param value La valeur à exclure
     * @param maxRowId Le nombre total de lignes
     * @return Un bitset avec les identifiants des lignes ne correspondant pas
     */
    public BitSet searchNot(T value, int maxRowId) {
        BitSet result = new BitSet(maxRowId);
        result.set(0, maxRowId); // Définit tous les bits à 1
        
        BitSet matching = bitmaps.get(value);
        if (matching != null) {
            // Soustrait les lignes correspondantes
            result.andNot(matching);
        }
        
        return result;
    }
    
    /**
     * Recherche les lignes dont la valeur est inférieure à la valeur spécifiée
     * Fonctionne uniquement pour les types comparables (nombres, chaînes, dates)
     * @param value La valeur de référence
     * @param maxRowId Le nombre total de lignes
     * @return Un bitset avec les identifiants des lignes correspondantes
     */
    public BitSet searchLessThan(Comparable<T> value, int maxRowId) {
        BitSet result = new BitSet(maxRowId);
        
        for (Map.Entry<T, BitSet> entry : bitmaps.entrySet()) {
            T entryValue = entry.getKey();
            if (entryValue != null && value.compareTo(entryValue) > 0) {
                result.or(entry.getValue());
            }
        }
        
        return result;
    }
    
    /**
     * Recherche les lignes dont la valeur est supérieure à la valeur spécifiée
     * @param value La valeur de référence
     * @param maxRowId Le nombre total de lignes
     * @return Un bitset avec les identifiants des lignes correspondantes
     */
    public BitSet searchGreaterThan(Comparable<T> value, int maxRowId) {
        BitSet result = new BitSet(maxRowId);
        
        for (Map.Entry<T, BitSet> entry : bitmaps.entrySet()) {
            T entryValue = entry.getKey();
            if (entryValue != null && value.compareTo(entryValue) < 0) {
                result.or(entry.getValue());
            }
        }
        
        return result;
    }
    
    /**
     * Supprime toutes les données de l'index
     */
    public void clear() {
        bitmaps.clear();
    }
    
    /**
     * Obtient la taille en mémoire approximative de l'index
     * @return La taille en octets
     */
    public long getMemoryUsage() {
        long size = 0;
        for (BitSet bs : bitmaps.values()) {
            // BitSet utilise des longs pour stocker les bits, donc 64 bits par long
            size += (bs.size() / 64) * 8;
        }
        return size;
    }
    
    /**
     * Récupère ou crée un bitmap pour une valeur
     * @param value La valeur
     * @return Le bitmap correspondant
     */
    private BitSet getOrCreateBitmap(T value) {
        return bitmaps.computeIfAbsent(value, k -> new BitSet());
    }
}
