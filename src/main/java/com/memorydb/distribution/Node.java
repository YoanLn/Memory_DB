package com.memorydb.distribution;

import java.util.Objects;

/**
 * Représente un nœud dans le cluster
 */
public class Node {
    private final String id;
    private final String address;
    
    /**
     * Crée un nouveau nœud
     * @param id L'identifiant du nœud
     * @param address L'adresse du nœud (format: "host:port")
     */
    public Node(String id, String address) {
        this.id = id;
        this.address = address;
    }
    
    /**
     * Obtient l'identifiant du nœud
     * @return L'identifiant
     */
    public String getId() {
        return id;
    }
    
    /**
     * Obtient l'adresse du nœud
     * @return L'adresse
     */
    public String getAddress() {
        return address;
    }
    
    /**
     * Construit l'URL REST pour ce nœud
     * @return L'URL REST
     */
    public String getRestUrl() {
        return "http://" + address;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(id, node.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return "Node{" +
                "id='" + id + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
} 