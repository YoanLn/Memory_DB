package com.memorydb.distribution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.memorydb.core.Column;
import com.memorydb.query.Condition;
import com.memorydb.query.Query;
import com.memorydb.rest.dto.ColumnDto;
import com.memorydb.rest.dto.ConditionDto;
import com.memorydb.rest.dto.QueryDto;
import com.memorydb.rest.dto.TableDto;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Client HTTP pour la communication entre les nœuds du cluster
 */
public class NodeClient {

    private static final Logger logger = LoggerFactory.getLogger(NodeClient.class);
    private static final Client client = ClientBuilder.newClient();
    private static final int FILE_TRANSFER_TIMEOUT_MS = 60000; // 60 secondes pour les transferts de fichiers
    
    /**
     * Vérifie si un fichier existe sur un nœud distant
     * @param node Le nœud distant
     * @param filePath Chemin du fichier à vérifier
     * @return true si le fichier existe, false sinon
     */
    public static boolean checkFileExists(NodeInfo node, String filePath) {
        try {
            String url = String.format("http://%s:%d/api/files/check", node.getAddress(), node.getPort());
            
            // Envoie la demande avec le chemin du fichier comme paramètre
            Response response = client.target(url)
                    .queryParam("path", filePath)
                    .request(MediaType.APPLICATION_JSON)
                    .get();
            
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                Map<String, Object> result = response.readEntity(new GenericType<Map<String, Object>>() {});
                boolean exists = Boolean.TRUE.equals(result.get("exists"));
                logger.info("Vérification du fichier sur le nœud {}: {} - {}", 
                        node.getId(), filePath, exists ? "existe" : "n'existe pas");
                return exists;
            }
            
            logger.error("Échec de la vérification du fichier sur le nœud {}: {} - {}", 
                    node.getId(), response.getStatus(), response.readEntity(String.class));
            return false;
        } catch (Exception e) {
            logger.error("Erreur lors de la vérification du fichier sur le nœud {}: {}", 
                    node.getId(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Envoie un fichier à un nœud distant
     * @param node Le nœud distant
     * @param filePath Chemin du fichier local à envoyer
     * @return Le chemin du fichier sur le nœud distant en cas de succès, null sinon
     */
    public static String sendFile(NodeInfo node, String filePath) {
        try {
            if (filePath == null || filePath.isEmpty()) {
                logger.error("Impossible d'envoyer un fichier avec un chemin vide");
                return null;
            }
            
            // Vérifie si le fichier existe déjà sur le nœud distant
            if (checkFileExists(node, filePath)) {
                logger.info("Le fichier existe déjà sur le nœud {}: {}", node.getId(), filePath);
                return filePath;
            }
            
            String url = String.format("http://%s:%d/api/files/upload", node.getAddress(), node.getPort());
            File file = new File(filePath);
            
            if (!file.exists()) {
                logger.error("Le fichier à envoyer n'existe pas: {}", filePath);
                return null;
            }
            
            // Préparation du formulaire multipart
            MultipartFormDataOutput formData = new MultipartFormDataOutput();
            formData.addFormData("file", new FileInputStream(file), MediaType.APPLICATION_OCTET_STREAM_TYPE);
            formData.addFormData("filename", file.getName(), MediaType.TEXT_PLAIN_TYPE);
            formData.addFormData("originalPath", filePath, MediaType.TEXT_PLAIN_TYPE);
            
            logger.info("Envoi du fichier {} au nœud {} ({} octets)", 
                    filePath, node.getId(), file.length());
            
            // Configuration du client avec timeout pour les gros fichiers
            Client clientWithTimeout = ClientBuilder.newBuilder()
                    .connectTimeout(FILE_TRANSFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .readTimeout(FILE_TRANSFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .build();
            
            try {
                // Envoie la demande
                Response response = clientWithTimeout.target(url)
                        .request()
                        .post(Entity.entity(formData, MediaType.MULTIPART_FORM_DATA_TYPE));
                
                if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                    Map<String, String> result = response.readEntity(new GenericType<Map<String, String>>() {});
                    String remotePath = result.get("path");
                    logger.info("Fichier envoyé avec succès au nœud {}, chemin distant: {}", 
                            node.getId(), remotePath);
                    return remotePath;
                }
                
                logger.error("Échec de l'envoi du fichier au nœud {}: {} - {}", 
                        node.getId(), response.getStatus(), response.readEntity(String.class));
            } finally {
                try {
                    clientWithTimeout.close();
                } catch (Exception e) {
                    logger.warn("Erreur lors de la fermeture du client HTTP: {}", e.getMessage());
                }
            }
            
            return null;
        } catch (Exception e) {
            logger.error("Erreur lors de l'envoi du fichier au nœud {}: {}", 
                    node.getId(), e.getMessage());
            return null;
        }
    }
    
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
            
            // Par défaut, c'est une requête distribuée
            queryDto.setDistributed(true);
            
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
     * Exécute une requête sur un nœud distant en utilisant un DTO existant
     * @param node Le nœud distant
     * @param queryDto La requête à exécuter
     * @return La liste des résultats
     */
    public static List<Map<String, Object>> executeQuery(NodeInfo node, QueryDto queryDto) {
        try {
            String url = String.format("http://%s:%d/api/query", node.getAddress(), node.getPort());
            
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
    
    /**
     * Récupère les statistiques d'une table sur un nœud distant
     * 
     * @param node Le nœud distant à interroger
     * @param tableName Le nom de la table à analyser
     * @return Les statistiques de la table (rowCount, columns, etc.), ou null en cas d'échec
     */
    public static Map<String, Object> getTableStats(NodeInfo node, String tableName) {
        try {
            // Construction de l'URL de l'endpoint des statistiques
            String url = String.format("http://%s:%d/api/tables/%s/stats", 
                    node.getAddress(), node.getPort(), tableName);
            
            logger.info("Récupération des statistiques pour la table '{}' sur le nœud {}", 
                    tableName, node.getId());
            
            // Envoie la requête GET
            Response response = client.target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .get();
            
            // Vérifie si la requête a réussi
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                // Lecture des statistiques de la réponse
                Map<String, Object> stats = response.readEntity(new GenericType<Map<String, Object>>() {});
                logger.debug("Statistiques de la table '{}' reçues du nœud {}", tableName, node.getId());
                return stats;
            } else {
                logger.warn("Impossible de récupérer les statistiques de la table '{}' sur le nœud {}: {} - {}", 
                        tableName, node.getId(), response.getStatus(), response.readEntity(String.class));
                return null;
            }
        } catch (Exception e) {
            logger.error("Erreur lors de la récupération des statistiques pour '{}' sur le nœud {}: {}", 
                    tableName, node.getId(), e.getMessage(), e);
            return null;
        }
    }
    
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
 /**
 * Exécute une requête sur un nœud distant en passant l'objet Query et QueryDto séparément
 * @param node Le nœud distant
 * @param query L'objet Query contenant les détails de la requête
 * @param originalQueryDto Le DTO original avec les flags de contrôle
 * @return La liste des résultats
 */
public static List<Map<String, Object>> executeQuery(NodeInfo node, Query query, QueryDto originalQueryDto) {
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
        
        // Crée un nouveau DTO de requête avec tous les champs nécessaires
        QueryDto queryDto = new QueryDto(
                query.getTableName(),
                query.getColumns(),
                conditionDtos,
                query.getOrderBy(),
                query.isOrderByAscending(),
                query.getLimit()
        );
        
        // Copie les flags importants du DTO original
        queryDto.setDistributed(false); // Important: Définir à false pour éviter des requêtes en cascade
        queryDto.setForwardedQuery(true); // Marque comme requête transmise pour éviter les boucles infinies
        
        // Important: Transfer group by and aggregation information
        if (originalQueryDto != null) {
            // Copy GROUP BY columns
            if (originalQueryDto.getGroupBy() != null && !originalQueryDto.getGroupBy().isEmpty()) {
                queryDto.setGroupBy(originalQueryDto.getGroupBy());
            }
            
            // Copy aggregation functions
            if (originalQueryDto.getAggregates() != null && !originalQueryDto.getAggregates().isEmpty()) {
                queryDto.setAggregates(originalQueryDto.getAggregates());
            }
        }
        
        logger.info("Envoi d'une requête transmise vers le nœud {}", node.getId());
        
        // Envoie la demande
        Response response = client.target(url)
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(queryDto, MediaType.APPLICATION_JSON));
        
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            try {
                // Lire la réponse comme une chaîne d'abord pour éviter l'erreur "Response is closed"
                String responseBody = response.readEntity(String.class);
                logger.info("Réponse reçue du nœud {}", node.getId());
                
                if (responseBody == null || responseBody.isEmpty() || responseBody.trim().equals("[]")) {
                    logger.info("Réponse vide reçue du nœud {}", node.getId());
                    return new ArrayList<>();
                }
                
                try {
                    // Convertir la chaîne en liste de maps
                    ObjectMapper mapper = new ObjectMapper();
                    List<Map<String, Object>> results = mapper.readValue(responseBody, 
                            new TypeReference<List<Map<String, Object>>>() {});
                    logger.info("{} résultats reçus du nœud {}", results.size(), node.getId());
                    return results;
                } catch (Exception e) {
                    logger.info("Impossible de convertir en liste directement, essai d'autres formats: {}", e.getMessage());
                    
                    try {
                        // Essaie de désérialiser en QueryResultDto ou autre format
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> resultMap = mapper.readValue(responseBody, 
                                new TypeReference<Map<String, Object>>() {});
                        
                        // Vérifie si le résultat contient le champ "rows" (format QueryResultDto)
                        if (resultMap.containsKey("rows") && resultMap.get("rows") instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<Map<String, Object>> rows = (List<Map<String, Object>>) resultMap.get("rows");
                            logger.info("Données extraites du champ 'rows': {} résultats", rows.size());
                            return rows;
                        }
                        
                        // Vérifie si le résultat contient un tableau de données sous "data"
                        if (resultMap.containsKey("data") && resultMap.get("data") instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<Map<String, Object>> dataList = (List<Map<String, Object>>) resultMap.get("data");
                            logger.info("Données extraites du champ 'data': {} résultats", dataList.size());
                            return dataList;
                        }
                        
                        logger.warn("Format de réponse non reconnu, retour d'une liste vide");
                        return new ArrayList<>();
                    } catch (Exception ex) {
                        logger.error("Impossible de traiter la réponse JSON: {}", ex.getMessage());
                        return new ArrayList<>();
                    }
                }
            } catch (Exception e) {
                logger.error("Erreur lors de la lecture de la réponse: {}", e.getMessage());
                return new ArrayList<>();
            }
        } else {
            try {
                // Lire le corps en tant que chaîne pour le logging, mais de manière sécurisée
                String errorBody = "[Impossible de lire le corps de l'erreur]";
                try {
                    errorBody = response.readEntity(String.class);
                } catch (Exception ex) {
                    // Ignore les erreurs lors de la lecture du corps d'erreur
                }
                
                logger.error("Échec de l'exécution de la requête sur le nœud {}: {} - {}", 
                        node.getId(), response.getStatus(), errorBody);
            } catch (Exception e) {
                logger.error("Échec de l'exécution de la requête sur le nœud {}: {}", 
                        node.getId(), response.getStatus());
            }
            return new ArrayList<>();
        }
    } catch (Exception e) {
        logger.error("Erreur lors de l'exécution de la requête sur le nœud {}: {}", 
                node.getId(), e.getMessage());
        return new ArrayList<>();
    }
}
}
