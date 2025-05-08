package com.memorydb.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Pool d'objets générique pour réduire les allocations et la pression du GC
 * @param <T> Le type d'objet à mettre en pool
 */
public class ObjectPool<T> {
    private final ConcurrentLinkedQueue<T> pool;
    private final Supplier<T> factory;
    private final int maxSize;
    private final AtomicInteger created = new AtomicInteger(0);
    private final AtomicInteger inUse = new AtomicInteger(0);
    
    /**
     * Crée un nouveau pool d'objets avec une taille maximale par défaut
     * @param factory Fournisseur d'objets à créer à la demande
     */
    public ObjectPool(Supplier<T> factory) {
        this(factory, 100);
    }
    
    /**
     * Crée un pool d'objets avec une taille maximale spécifiée
     * @param factory Fournisseur d'objets à créer à la demande
     * @param maxSize Taille maximale du pool
     */
    public ObjectPool(Supplier<T> factory, int maxSize) {
        this.factory = factory;
        this.maxSize = maxSize;
        this.pool = new ConcurrentLinkedQueue<>();
    }
    
    /**
     * Emprunte un objet du pool ou en crée un nouveau si nécessaire
     * @return Un objet du type T
     */
    public T borrow() {
        T object = pool.poll();
        if (object == null) {
            object = factory.get();
            created.incrementAndGet();
        }
        inUse.incrementAndGet();
        return object;
    }
    
    /**
     * Retourne un objet au pool pour réutilisation ultérieure
     * @param object L'objet à retourner
     */
    public void release(T object) {
        if (object != null) {
            inUse.decrementAndGet();
            
            // Limite la taille du pool pour éviter la croissance incontrôlée
            if (pool.size() < maxSize) {
                pool.offer(object);
            }
        }
    }
    
    /**
     * Obtient le nombre total d'objets créés par ce pool
     * @return Le nombre d'objets créés
     */
    public int getCreatedCount() {
        return created.get();
    }
    
    /**
     * Obtient le nombre d'objets actuellement en utilisation 
     * @return Le nombre d'objets en utilisation
     */
    public int getInUseCount() {
        return inUse.get();
    }
    
    /**
     * Obtient le nombre d'objets actuellement disponibles dans le pool
     * @return Le nombre d'objets disponibles
     */
    public int getAvailableCount() {
        return pool.size();
    }
    
    /**
     * Vide le pool et libère tous les objets inactifs
     */
    public void clear() {
        pool.clear();
    }
}
