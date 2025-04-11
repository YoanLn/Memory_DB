package com.memorydb.distribution;

import java.util.Objects;

/**
 * Classe qui représente les informations d'un nœud dans le cluster
 */
public class NodeInfo {

    private final String id;
    private final String address;
    private final int port;
    private long lastHeartbeat;
    private NodeStatus status;
    
    /**
     * Constructeur
     * @param id L'identifiant unique du nœud
     * @param address L'adresse du nœud
     * @param port Le port du nœud
     */
    public NodeInfo(String id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
        this.lastHeartbeat = System.currentTimeMillis();
        this.status = NodeStatus.ONLINE;
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
     * Obtient le port du nœud
     * @return Le port
     */
    public int getPort() {
        return port;
    }
    
    /**
     * Obtient le timestamp du dernier battement de cœur
     * @return Le timestamp
     */
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }
    
    /**
     * Obtient le timestamp du dernier moment où le nœud a été vu
     * @return Le timestamp
     */
    public long getLastSeen() {
        return lastHeartbeat;
    }
    
    /**
     * Met à jour le timestamp du dernier battement de cœur
     */
    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
        this.status = NodeStatus.ONLINE;
    }
    
    /**
     * Obtient le statut du nœud
     * @return Le statut
     */
    public NodeStatus getStatus() {
        return status;
    }
    
    /**
     * Définit le statut du nœud
     * @param status Le nouveau statut
     */
    public void setStatus(NodeStatus status) {
        this.status = status;
    }
    
    /**
     * Vérifie si le nœud est en ligne
     * @return true si le nœud est en ligne, false sinon
     */
    public boolean isOnline() {
        return status == NodeStatus.ONLINE;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return Objects.equals(id, nodeInfo.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return "NodeInfo{" +
                "id='" + id + '\'' +
                ", address='" + address + '\'' +
                ", port=" + port +
                ", status=" + status +
                '}';
    }
} 