package com.memorydb.distribution;

import com.memorydb.core.Column;
import com.memorydb.query.Condition;
import com.memorydb.query.Query;
import com.memorydb.rest.dto.ColumnDto;
import com.memorydb.rest.dto.ConditionDto;
import com.memorydb.rest.dto.QueryDto;
import com.memorydb.rest.dto.TableDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Client HTTP pour la communication entre les nœuds du cluster
 */
public class NodeClient {

    private static final Logger logger = LoggerFactory.getLogger(NodeClient.class);
    private static final Client client = ClientBuilder.newClient();
    
    /**
     * Envoie une demande de création de table à un nœud distant
     * @param node Le nœud distant
     * @param tableName Le nom de la table
     * @param columns Les colonnes de la table
     * @return true si la demande a réussi, false sinon
     */
    public static boolean sendTableCreationRequest(NodeInfo node, String tableName, List<Column> columns) {
        try {
            String url = String.format("http://%s:%d/api/tables", node.getAddress(), node.getPort());
            
            // Convertit les colonnes en DTO
            List<ColumnDto> columnDtos = columns.stream()
                    .map(column -> new ColumnDto(
                            column.getName(),
                            column.getType().name(),
                            column.isNullable()))
                    .collect(Collectors.toList());
            
            // Crée le DTO de la table
            TableDto tableDto = new TableDto(tableName, columnDtos);
            
            // Envoie la demande
            Response response = client.target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(tableDto, MediaType.APPLICATION_JSON));
            
            boolean success = response.getStatus() == Response.Status.CREATED.getStatusCode();
            if (!success) {
                logger.error("Échec de la création de la table '{}' sur le nœud {}: {} - {}", 
                        tableName, node.getId(), response.getStatus(), response.readEntity(String.class));
            }
            
            return success;
        } catch (Exception e) {
            logger.error("Erreur lors de l'envoi de la demande de création de table au nœud {}: {}", 
                    node.getId(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Envoie une demande de suppression de table à un nœud distant
     * @param node Le nœud distant
     * @param tableName Le nom de la table
     * @return true si la demande a réussi, false sinon
     */
    public static boolean sendTableDeletionRequest(NodeInfo node, String tableName) {
        try {
            String url = String.format("http://%s:%d/api/tables/%s", node.getAddress(), node.getPort(), tableName);
            
            // Envoie la demande
            Response response = client.target(url)
                    .request()
                    .delete();
            
            boolean success = response.getStatus() == Response.Status.NO_CONTENT.getStatusCode();
            if (!success) {
                logger.error("Échec de la suppression de la table '{}' sur le nœud {}: {} - {}", 
                        tableName, node.getId(), response.getStatus(), response.readEntity(String.class));
            }
            
            return success;
        } catch (Exception e) {
            logger.error("Erreur lors de l'envoi de la demande de suppression de table au nœud {}: {}", 
                    node.getId(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Exécute une requête sur un nœud distant
     * @param node Le nœud distant
     * @param query La requête à exécuter
     * @return La liste des résultats
     */
    public static List<Map<String, Object>> executeQuery(NodeInfo node, Query query) {
        try {
            String url = String.format("http://%s:%d/api/query", node.getAddress(), node.getPort());
            
            // Convertit les conditions en ConditionDto
            List<ConditionDto> conditionDtos = null;
            if (query.getConditions() != null && !query.getConditions().isEmpty()) {
                conditionDtos = query.getConditions().stream()
                        .map(condition -> {
                            Condition.Operator operator = condition.getOperator();
                            String operatorStr = operator.name();
                            return new ConditionDto(
                                    condition.getColumnName(),
                                    operatorStr,
                                    condition.getValue());
                        })
                        .collect(Collectors.toList());
            }
            
            // Convertit la requête en DTO
            QueryDto queryDto = new QueryDto(
                    query.getTableName(),
                    query.getColumns(),
                    conditionDtos,
                    query.getOrderBy(),
                    query.isOrderByAscending(),
                    query.getLimit()
            );
            
            // Envoie la demande
            Response response = client.target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(queryDto, MediaType.APPLICATION_JSON));
            
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(new GenericType<List<Map<String, Object>>>() {});
            } else {
                logger.error("Échec de l'exécution de la requête sur le nœud {}: {} - {}", 
                        node.getId(), response.getStatus(), response.readEntity(String.class));
                return new ArrayList<>();
            }
        } catch (Exception e) {
            logger.error("Erreur lors de l'exécution de la requête sur le nœud {}: {}", 
                    node.getId(), e.getMessage());
            return new ArrayList<>();
        }
    }
} 