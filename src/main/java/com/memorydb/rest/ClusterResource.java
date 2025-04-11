package com.memorydb.rest;

import com.memorydb.distribution.ClusterManager;
import com.memorydb.distribution.NodeInfo;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Resource REST pour les informations du cluster
 */
@Path("/api/cluster")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    
    @Inject
    private ClusterManager clusterManager;
    
    @ConfigProperty(name = "quarkus.application.version", defaultValue = "1.0.0")
    private String appVersion;
    
    /**
     * Obtient les informations sur le nœud local
     * @return Les informations du nœud
     */
    @GET
    @Path("/info")
    public Response getNodeInfo() {
        try {
            NodeInfo localNode = clusterManager.getLocalNode();
            
            Map<String, Object> nodeInfo = new HashMap<>();
            nodeInfo.put("id", localNode.getId());
            nodeInfo.put("address", localNode.getAddress());
            nodeInfo.put("port", localNode.getPort());
            nodeInfo.put("status", localNode.getStatus().name());
            nodeInfo.put("version", appVersion);
            
            return Response.ok(nodeInfo).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la récupération des informations du nœud: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Obtient les informations sur tous les nœuds du cluster
     * @return La liste des nœuds
     */
    @GET
    @Path("/nodes")
    public Response getClusterNodes() {
        try {
            Collection<NodeInfo> nodes = clusterManager.getAllNodes();
            Collection<Map<String, Object>> nodeInfos = new ArrayList<>();
            
            for (NodeInfo node : nodes) {
                Map<String, Object> nodeInfo = new HashMap<>();
                nodeInfo.put("id", node.getId());
                nodeInfo.put("address", node.getAddress());
                nodeInfo.put("port", node.getPort());
                nodeInfo.put("status", node.getStatus().name());
                nodeInfo.put("isLocal", node.getId().equals(clusterManager.getLocalNode().getId()));
                
                nodeInfos.add(nodeInfo);
            }
            
            return Response.ok(nodeInfos).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la récupération des nœuds: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Obtient des statistiques sur le cluster
     * @return Les statistiques
     */
    @GET
    @Path("/stats")
    public Response getClusterStats() {
        try {
            Collection<NodeInfo> nodes = clusterManager.getAllNodes();
            
            Map<String, Object> stats = new HashMap<>();
            stats.put("nodeCount", nodes.size());
            
            int onlineNodes = 0;
            int offlineNodes = 0;
            
            for (NodeInfo node : nodes) {
                if (node.isOnline()) {
                    onlineNodes++;
                } else {
                    offlineNodes++;
                }
            }
            
            stats.put("onlineNodes", onlineNodes);
            stats.put("offlineNodes", offlineNodes);
            stats.put("localNodeId", clusterManager.getLocalNode().getId());
            
            return Response.ok(stats).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la récupération des statistiques: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Vérifie la santé du cluster
     * @return Une réponse HTTP
     */
    @GET
    @Path("/health")
    public Response checkClusterHealth() {
        try {
            Collection<NodeInfo> nodes = clusterManager.getAllNodes();
            int onlineNodes = 0;
            
            for (NodeInfo node : nodes) {
                if (node.isOnline()) {
                    onlineNodes++;
                }
            }
            
            boolean isHealthy = onlineNodes > 0;
            
            Map<String, Object> health = new HashMap<>();
            health.put("status", isHealthy ? "UP" : "DOWN");
            health.put("totalNodes", nodes.size());
            health.put("onlineNodes", onlineNodes);
            
            return Response.ok(health).build();
        } catch (Exception e) {
            Map<String, Object> health = new HashMap<>();
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
            
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(health)
                    .build();
        }
    }
} 