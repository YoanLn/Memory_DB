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
     * Exécute une requête sur un nœud distant en mode transmis (forwarded) pour éviter les boucles infinies
     * @param node Le nœud distant
     * @param query La requête à exécuter
     * @return La liste des résultats
     */
    public static List<Map<String, Object>> executeForwardedQuery(NodeInfo node, Query query) {
        try {
            String url = String.format("http://%s:%d/api/query/forwarded", node.getAddress(), node.getPort());
            
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
                    query.getLimit(),
                    false  // La requête n'est plus distribuée pour éviter les boucles infinies
            );
            
            // Envoie la demande au endpoint spécial pour les requêtes transmises
            Response response = client.target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(queryDto, MediaType.APPLICATION_JSON));
            
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(new GenericType<List<Map<String, Object>>>() {});
            } else {
                logger.error("Échec de l'exécution de la requête transmise sur le nœud {}: {} - {}", 
                        node.getId(), response.getStatus(), response.readEntity(String.class));
                return new ArrayList<>();
            }
        } catch (Exception e) {
            logger.error("Erreur lors de l'exécution de la requête transmise sur le nœud {}: {}", 
                    node.getId(), e.getMessage());
            return new ArrayList<>();
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
            // IMPORTANT: Marquer explicitement comme NON-distribuée pour éviter les boucles infinies
            // entre les nœuds (sinon chaque nœud demande à tous les autres nœuds, etc.)
            QueryDto queryDto = new QueryDto(
                    query.getTableName(),
                    query.getColumns(),
                    conditionDtos,
                    query.getOrderBy(),
                    query.isOrderByAscending(),
                    query.getLimit(),
                    false  // Force la requête à être non-distribuée pour les nœuds distants
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
    
    /**
     * Envoie une demande de chargement d'un segment de fichier Parquet à un nœud distant
     * @param node Le nœud distant
     * @param tableName Le nom de la table
     * @param payload Les paramètres de chargement (filePath, startRow, rowCount, etc.)
     * @return Le nombre de lignes chargées sur le nœud distant, ou 0 en cas d'erreur
     */
    public static long sendLoadParquetSegmentRequest(NodeInfo node, String tableName, Map<String, Object> payload) {
        try {
            String url = String.format("http://%s:%d/api/tables/%s/load-parquet-segment", 
                    node.getAddress(), node.getPort(), tableName);
            
            logger.info("Envoi d'une demande de chargement de segment au nœud {}: {} lignes à partir de {}",
                    node.getId(), payload.get("rowCount"), payload.get("startRow"));
            
            // Envoie la demande
            Response response = client.target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(payload, MediaType.APPLICATION_JSON));
            
            boolean success = response.getStatus() == Response.Status.OK.getStatusCode();
            if (success) {
                // Extraction du nombre réel de lignes chargées depuis la réponse
                try {
                    Map<String, Object> result = response.readEntity(new GenericType<Map<String, Object>>() {});
                    if (result.containsKey("loadedRows")) {
                        long loadedRows = ((Number)result.get("loadedRows")).longValue();
                        logger.info("Le nœud {} a chargé {} lignes avec succès", node.getId(), loadedRows);
                        return loadedRows;
                    } else {
                        logger.warn("Le nœud {} n'a pas retourné le nombre de lignes chargées, utilisation de l'approximation", node.getId());
                        return payload.containsKey("rowCount") ? ((Number)payload.get("rowCount")).longValue() : 0;
                    }
                } catch (Exception e) {
                    logger.warn("Erreur lors de la lecture de la réponse du nœud {}: {}", node.getId(), e.getMessage());
                    return payload.containsKey("rowCount") ? ((Number)payload.get("rowCount")).longValue() : 0;
                }
            } else {
                logger.error("Échec du chargement du segment Parquet sur le nœud {}: {} - {}", 
                        node.getId(), response.getStatus(), response.readEntity(String.class));
                return 0;
            }
        } catch (Exception e) {
            logger.error("Erreur lors de l'envoi de la demande de chargement Parquet au nœud {}: {}", 
                    node.getId(), e.getMessage());
            return 0;
        }
    }
    
    /**
     * Envoie une demande de chargement d'une ligne spécifique d'un fichier Parquet à un nœud distant
     * @param node Le nœud distant
     * @param tableName Le nom de la table où charger la ligne
     * @param payload Les paramètres (filePath, rowIndex)
     * @return true si la demande a réussi, false sinon
     */
    public static boolean sendLoadSpecificRowRequest(NodeInfo node, String tableName, Map<String, Object> payload) {
        try {
            String url = String.format("http://%s:%d/api/tables/%s/load-specific-row", 
                    node.getAddress(), node.getPort(), tableName);
            
            logger.info("Envoi d'une demande de chargement de ligne spécifique au nœud {}: ligne {}",
                    node.getId(), payload.get("rowIndex"));
            
            // Envoie la demande
            Response response = client.target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(payload, MediaType.APPLICATION_JSON));
            
            boolean success = response.getStatus() == Response.Status.OK.getStatusCode();
            if (!success) {
                logger.error("Échec du chargement de la ligne sur le nœud {}: {} - {}", 
                        node.getId(), response.getStatus(), response.readEntity(String.class));
            }
            
            return success;
        } catch (Exception e) {
            logger.error("Erreur lors de l'envoi de la demande de chargement de ligne au nœud {}: {}", 
                    node.getId(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Envoie une demande de chargement d'une plage de lignes d'un fichier Parquet à un nœud distant
     * Méthode optimisée pour les grands volumes de données permettant le chargement par lots
     * 
     * @param node Le nœud distant
     * @param tableName Le nom de la table
     * @param filePath Chemin du fichier Parquet
     * @param startRow Première ligne à charger (0-based)
     * @param rowCount Nombre de lignes à charger
     * @param batchSize Taille des lots pour le chargement
     * @return true si la demande a réussi, false sinon
     */
    public static boolean sendLoadRangeRequest(NodeInfo node, String tableName, String filePath, 
                                                int startRow, int rowCount, int batchSize) {
        try {
            String url = String.format("http://%s:%d/api/tables/%s/load-range", 
                    node.getAddress(), node.getPort(), tableName);
            
            // Prépare les paramètres
            Map<String, Object> payload = Map.of(
                "filePath", filePath,
                "startRow", startRow,
                "rowCount", rowCount,
                "batchSize", batchSize
            );
            
            logger.info("Envoi d'une demande de chargement par lot au nœud {}: {} lignes à partir de {}",
                    node.getId(), rowCount, startRow);
            
            // Envoie la demande
            Response response = client.target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(payload, MediaType.APPLICATION_JSON));
            
            boolean success = response.getStatus() == Response.Status.OK.getStatusCode();
            if (!success) {
                logger.error("Échec du chargement par lot sur le nœud {}: {} - {}", 
                        node.getId(), response.getStatus(), response.readEntity(String.class));
            } else {
                logger.info("Chargement par lot réussi sur le nœud {}: {} lignes", node.getId(), rowCount);
            }
            
            return success;
        } catch (Exception e) {
            logger.error("Erreur lors de l'envoi de la demande de chargement par lot au nœud {}: {}", 
                    node.getId(), e.getMessage());
            return false;
        }
    }
} 