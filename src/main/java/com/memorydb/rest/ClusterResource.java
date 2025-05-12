package com.memorydb.rest;

import com.memorydb.distribution.ClusterManager;
import com.memorydb.distribution.NodeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Ressource REST pour la gestion du cluster
 */
@Path("/api/cluster")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ClusterResource {
    
    private static final Logger logger = LoggerFactory.getLogger(ClusterResource.class);
    
    @Inject
    private ClusterManager clusterManager;
    
    /**
     * Vérifie l'état de santé du cluster
     * @return Informations sur l'état du cluster
     */
    @GET
    @Path("/health")
    public Response getClusterHealth() {
        try {
            Collection<NodeInfo> nodes = clusterManager.getAllNodes();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "UP");
            response.put("nodesCount", nodes.size());
            
            // Information sur le nœud local
            NodeInfo localNode = clusterManager.getLocalNode();
            response.put("localNode", mapNodeInfo(localNode));
            
            // Informations sur tous les nœuds
            Map<String, Object> nodesInfo = new HashMap<>();
            for (NodeInfo node : nodes) {
                nodesInfo.put(node.getId(), mapNodeInfo(node));
            }
            response.put("nodes", nodesInfo);
            
            return Response.ok(response).build();
        } catch (Exception e) {
            logger.error("Erreur lors de la vérification de l'état du cluster: {}", e.getMessage(), e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "DOWN");
            errorResponse.put("error", e.getMessage());
            
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(errorResponse)
                    .build();
        }
    }
    
    /**
     * Convertit un NodeInfo en Map pour la sérialisation JSON
     */
    private Map<String, Object> mapNodeInfo(NodeInfo node) {
        Map<String, Object> nodeMap = new HashMap<>();
        nodeMap.put("id", node.getId());
        nodeMap.put("address", node.getAddress());
        nodeMap.put("port", node.getPort());
        nodeMap.put("status", node.getStatus().name());
        return nodeMap;
    }
}
