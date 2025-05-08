package com.memorydb.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Gestionnaire de pools d'objets fréquemment utilisés dans l'application
 * Optimisation pour réduire la pression sur le garbage collector
 * et améliorer les performances en réutilisant les objets
 */
public class ObjectPools {
    // Pool pour les HashMap utilisés dans les résultats de requêtes
    private static final ObjectPool<HashMap<String, Object>> ROW_MAP_POOL = 
        new ObjectPool<>(() -> new HashMap<>(16), 1000);
    
    // Pool pour les tableaux d'objets (lignes)
    private static final ObjectPool<Object[]> ROW_BUFFER_POOL = 
        new ObjectPool<>(() -> new Object[50], 1000);
    
    // Pool pour les ArrayList
    private static final ObjectPool<ArrayList<Object>> LIST_POOL = 
        new ObjectPool<>(() -> new ArrayList<>(16), 500);
    
    /**
     * Emprunte une HashMap du pool
     * @return Une HashMap réinitialisée prête à l'emploi
     */
    public static HashMap<String, Object> borrowMap() {
        return ROW_MAP_POOL.borrow();
    }
    
    /**
     * Retourne une HashMap au pool après utilisation
     * @param map La HashMap à retourner au pool
     */
    public static void returnMap(HashMap<String, Object> map) {
        if (map != null) {
            map.clear();
            ROW_MAP_POOL.release(map);
        }
    }
    
    /**
     * Emprunte un tableau d'objets du pool
     * @param size La taille minimale requise
     * @return Un tableau d'objets de la taille appropriée
     */
    public static Object[] borrowArray(int size) {
        Object[] array = ROW_BUFFER_POOL.borrow();
        if (array.length < size) {
            // Si le tableau est trop petit, en créer un nouveau
            // (ne pas retourner le trop petit au pool pour éviter les fuites)
            array = new Object[size];
        }
        return array;
    }
    
    /**
     * Retourne un tableau d'objets au pool
     * @param array Le tableau à retourner
     */
    public static void returnArray(Object[] array) {
        if (array != null) {
            // Effacer les références pour éviter les fuites mémoire
            for (int i = 0; i < array.length; i++) {
                array[i] = null;
            }
            ROW_BUFFER_POOL.release(array);
        }
    }
    
    /**
     * Emprunte une ArrayList du pool
     * @return Une ArrayList vide
     */
    @SuppressWarnings("unchecked")
    public static <T> ArrayList<T> borrowList() {
        ArrayList<Object> list = LIST_POOL.borrow();
        return (ArrayList<T>) list;
    }
    
    /**
     * Retourne une ArrayList au pool
     * @param list La liste à retourner
     */
    @SuppressWarnings("unchecked")
    public static <T> void returnList(ArrayList<T> list) {
        if (list != null) {
            list.clear();
            LIST_POOL.release((ArrayList<Object>) list);
        }
    }
    
    /**
     * Obtient des statistiques sur l'utilisation des pools
     * @return Une Map avec les statistiques de chaque pool
     */
    public static Map<String, Map<String, Integer>> getStats() {
        Map<String, Map<String, Integer>> stats = new HashMap<>();
        
        Map<String, Integer> rowMapStats = new HashMap<>();
        rowMapStats.put("created", ROW_MAP_POOL.getCreatedCount());
        rowMapStats.put("inUse", ROW_MAP_POOL.getInUseCount());
        rowMapStats.put("available", ROW_MAP_POOL.getAvailableCount());
        
        Map<String, Integer> rowBufferStats = new HashMap<>();
        rowBufferStats.put("created", ROW_BUFFER_POOL.getCreatedCount());
        rowBufferStats.put("inUse", ROW_BUFFER_POOL.getInUseCount());
        rowBufferStats.put("available", ROW_BUFFER_POOL.getAvailableCount());
        
        Map<String, Integer> listStats = new HashMap<>();
        listStats.put("created", LIST_POOL.getCreatedCount());
        listStats.put("inUse", LIST_POOL.getInUseCount());
        listStats.put("available", LIST_POOL.getAvailableCount());
        
        stats.put("rowMaps", rowMapStats);
        stats.put("rowBuffers", rowBufferStats);
        stats.put("lists", listStats);
        
        return stats;
    }
    
    /**
     * Vide tous les pools et libère les objets non utilisés
     * À utiliser avec précaution, uniquement lorsque l'application est inactive
     */
    public static void clearAll() {
        ROW_MAP_POOL.clear();
        ROW_BUFFER_POOL.clear();
        LIST_POOL.clear();
    }
}
