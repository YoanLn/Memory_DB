package com.memorydb.rest;

import com.memorydb.common.DataType;
import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.distribution.ClusterManager;
import com.memorydb.distribution.DistributedParquetLoader;
import com.memorydb.parquet.ParquetLoadOptions;
import com.memorydb.parquet.ParquetLoader;
import com.memorydb.parquet.ParquetLoadStats;
import com.memorydb.parquet.VectorizedParquetLoader;
import com.memorydb.rest.dto.ColumnDto;
import com.memorydb.rest.dto.TableDto;
import com.memorydb.storage.ColumnStore;
import com.memorydb.storage.TableData;

import org.jboss.resteasy.annotations.providers.multipart.MultipartForm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resource REST pour la gestion des tables
 */
@Path("/api/tables")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class TableResource {
    
    private static final Logger logger = LoggerFactory.getLogger(TableResource.class);
    
    @Inject
    private DatabaseContext databaseContext;
    
    @Inject
    private ClusterManager clusterManager;
    
    @Inject
    private ParquetLoader parquetLoader;
    
    @Inject
    private VectorizedParquetLoader vectorizedParquetLoader;
    
    @Inject
    private DistributedParquetLoader distributedParquetLoader;
    
    /**
     * Crée une nouvelle table
     * @param tableDto Les informations de la table
     * @return La réponse HTTP
     */
    @POST
    public Response createTable(TableDto tableDto) {
        try {
            // Vérifie que les paramètres sont valides
            if (tableDto.getName() == null || tableDto.getName().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le nom de la table est obligatoire")
                        .build();
            }
            
            if (tableDto.getColumns() == null || tableDto.getColumns().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Au moins une colonne doit être définie")
                        .build();
            }
            
            // Vérifie si la table existe déjà
            if (databaseContext.tableExists(tableDto.getName())) {
                return Response.status(Response.Status.CONFLICT)
                        .entity("La table '" + tableDto.getName() + "' existe déjà")
                        .build();
            }
            
            // Convertit les DTO en objets métier
            List<Column> columns = new ArrayList<>();
            for (ColumnDto columnDto : tableDto.getColumns()) {
                DataType dataType = DataType.fromString(columnDto.getType());
                Column column = new Column(columnDto.getName(), dataType, columnDto.isNullable());
                columns.add(column);
            }
            
            // Crée la table
            Table table = databaseContext.createTable(tableDto.getName(), columns);
            
            // Synchronise la création avec les autres nœuds (si en mode distribué)
            clusterManager.syncTableCreation(tableDto.getName(), columns);
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("name", table.getName());
            response.put("columnsCount", table.getColumns().size());
            response.put("message", "Table créée avec succès");
            
            return Response.status(Response.Status.CREATED)
                    .entity(response)
                    .build();
                    
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la création de la table: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Charge un fichier Parquet dans une table existante
     * @param tableName Le nom de la table
     * @param payload Les informations pour le chargement
     * @return La réponse HTTP
     */
    @POST
    @Path("/{tableName}/load-parquet")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response loadParquetFile(
            @PathParam("tableName") String tableName,
            Map<String, Object> payload) {
        try {
            String filePath = (String) payload.get("filePath");
            
            // Vérifie que le chemin du fichier est fourni
            if (filePath == null || filePath.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le chemin du fichier Parquet est obligatoire")
                        .build();
            }
            
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            // Récupère la limite de lignes (facultatif)
            int rowLimit = -1; // -1 = pas de limite
            if (payload.containsKey("rowLimit")) {
                try {
                    Object limitObj = payload.get("rowLimit");
                    if (limitObj instanceof Integer) {
                        rowLimit = (Integer) limitObj;
                    } else if (limitObj instanceof String) {
                        rowLimit = Integer.parseInt((String) limitObj);
                    } else if (limitObj instanceof Number) {
                        rowLimit = ((Number) limitObj).intValue();
                    }
                } catch (NumberFormatException e) {
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity("La limite de lignes doit être un nombre entier")
                            .build();
                }
            }
            
            // Charge le fichier Parquet avec la limite spécifiée
            int rowCount = parquetLoader.loadParquetFile(tableName, filePath, rowLimit);
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("filePath", filePath);
            response.put("rowsLoaded", rowCount);
            if (rowLimit > 0) {
                response.put("rowLimit", rowLimit);
            }
            response.put("message", "Fichier Parquet chargé avec succès");
            
            return Response.ok(response).build();
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors du chargement du fichier Parquet: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Crée une table à partir d'un fichier Parquet
     * @param payload Les informations pour la création
     * @return La réponse HTTP
     */
    @POST
    @Path("/from-parquet")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createTableFromParquet(Map<String, Object> payload) {
        try {
            String tableName = (String) payload.get("tableName");
            String filePath = (String) payload.get("filePath");
            
            // Vérifie que les paramètres sont valides
            if (tableName == null || tableName.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le nom de la table est obligatoire")
                        .build();
            }
            
            if (filePath == null || filePath.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le chemin du fichier Parquet est obligatoire")
                        .build();
            }
            
            // Vérifie si la table existe déjà
            if (databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.CONFLICT)
                        .entity("La table '" + tableName + "' existe déjà")
                        .build();
            }
            
            // Récupère la limite de lignes (facultatif)
            int rowLimit = -1; // -1 = pas de limite
            if (payload.containsKey("rowLimit")) {
                try {
                    Object limitObj = payload.get("rowLimit");
                    if (limitObj instanceof Integer) {
                        rowLimit = (Integer) limitObj;
                    } else if (limitObj instanceof String) {
                        rowLimit = Integer.parseInt((String) limitObj);
                    } else if (limitObj instanceof Number) {
                        rowLimit = ((Number) limitObj).intValue();
                    }
                } catch (NumberFormatException e) {
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity("La limite de lignes doit être un nombre entier")
                            .build();
                }
            }
            
            // Récupère le paramètre loadData (facultatif)
            boolean loadData = true; // Par défaut, on charge les données
            if (payload.containsKey("loadData")) {
                Object loadDataObj = payload.get("loadData");
                if (loadDataObj instanceof Boolean) {
                    loadData = (Boolean) loadDataObj;
                } else if (loadDataObj instanceof String) {
                    loadData = Boolean.parseBoolean((String) loadDataObj);
                }
            }
            
            // Crée la table à partir du fichier Parquet avec la limite spécifiée et l'option de chargement
            Table table = parquetLoader.createTableFromParquet(tableName, filePath, rowLimit, loadData);
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("name", table.getName());
            response.put("columnsCount", table.getColumns().size());
            if (rowLimit > 0) {
                response.put("rowLimit", rowLimit);
            }
            response.put("dataLoaded", loadData);
            response.put("message", loadData 
                ? "Table créée avec succès à partir du fichier Parquet" 
                : "Structure de table créée avec succès à partir du fichier Parquet (sans données)");
            
            return Response.status(Response.Status.CREATED)
                    .entity(response)
                    .build();
                    
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la création de la table: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Crée uniquement le schéma d'une table à partir d'un fichier Parquet (sans charger les données)
     * @param payload Les informations pour la création
     * @return La réponse HTTP
     */
    @POST
    @Path("/schema-from-parquet")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createTableSchemaFromParquet(Map<String, Object> payload) {
        try {
            String tableName = (String) payload.get("tableName");
            String filePath = (String) payload.get("filePath");
            
            // Vérifie que les paramètres sont valides
            if (tableName == null || tableName.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le nom de la table est obligatoire")
                        .build();
            }
            
            if (filePath == null || filePath.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le chemin du fichier Parquet est obligatoire")
                        .build();
            }
            
            // Vérifie si la table existe déjà
            if (databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.CONFLICT)
                        .entity("La table '" + tableName + "' existe déjà")
                        .build();
            }
            
            // Crée uniquement le schéma de la table à partir du fichier Parquet
            Table table = parquetLoader.createTableSchemaFromParquet(tableName, filePath);
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("name", table.getName());
            response.put("columnsCount", table.getColumns().size());
            response.put("message", "Structure de table créée avec succès à partir du fichier Parquet (sans données)");
            
            return Response.status(Response.Status.CREATED)
                    .entity(response)
                    .build();
                    
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la création du schéma de la table: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Supprime une table
     * @param tableName Le nom de la table
     * @return La réponse HTTP
     */
    @DELETE
    @Path("/{tableName}")
    public Response deleteTable(@PathParam("tableName") String tableName) {
        try {
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            // Supprime la table
            databaseContext.dropTable(tableName);
            
            // Synchronise la suppression avec les autres nœuds (si en mode distribué)
            clusterManager.syncTableDeletion(tableName);
            
            return Response.status(Response.Status.NO_CONTENT)
                    .build();
                    
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la suppression de la table: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Obtient la liste des tables
     * @return La liste des tables
     */
    @GET
    public Response listTables() {
        try {
            List<Table> tables = databaseContext.getAllTables();
            List<Map<String, Object>> result = new ArrayList<>();
            
            for (Table table : tables) {
                Map<String, Object> tableInfo = new HashMap<>();
                tableInfo.put("name", table.getName());
                
                List<Map<String, Object>> columnsInfo = new ArrayList<>();
                for (Column column : table.getColumns()) {
                    Map<String, Object> columnInfo = new HashMap<>();
                    columnInfo.put("name", column.getName());
                    columnInfo.put("type", column.getType().name());
                    columnInfo.put("nullable", column.isNullable());
                    columnsInfo.add(columnInfo);
                }
                
                tableInfo.put("columns", columnsInfo);
                result.add(tableInfo);
            }
            
            return Response.ok(result).build();
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la récupération des tables: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Obtient les informations d'une table
     * @param tableName Le nom de la table
     * @return Les informations de la table
     */
    @GET
    @Path("/{tableName}")
    public Response getTable(@PathParam("tableName") String tableName) {
        try {
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            Table table = databaseContext.getTable(tableName);
            
            Map<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("name", table.getName());
            
            List<Map<String, Object>> columnsInfo = new ArrayList<>();
            for (Column column : table.getColumns()) {
                Map<String, Object> columnInfo = new HashMap<>();
                columnInfo.put("name", column.getName());
                columnInfo.put("type", column.getType().name());
                columnInfo.put("nullable", column.isNullable());
                columnsInfo.add(columnInfo);
            }
            
            tableInfo.put("columns", columnsInfo);
            
            return Response.ok(tableInfo).build();
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la récupération de la table: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Diagnostic des données d'une table - accès direct aux données stockées
     * @param tableName Le nom de la table
     * @param limit Le nombre maximum de lignes à renvoyer
     * @return Les données de la table
     */
    @GET
    @Path("/{tableName}/data")
    public Response getTableData(
            @PathParam("tableName") String tableName,
            @QueryParam("limit") @DefaultValue("10") int limit) {
        try {
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            Table table = databaseContext.getTable(tableName);
            TableData tableData = databaseContext.getTableData(tableName);
            List<Column> columns = table.getColumns();
            
            // Acquiert un verrou en lecture
            tableData.readLock();
            try {
                // Récupère les données directement des ColumnStore
                List<Map<String, Object>> rows = new ArrayList<>();
                int rowCount = Math.min(tableData.getRowCount(), limit);
                
                for (int i = 0; i < rowCount; i++) {
                    Map<String, Object> row = new HashMap<>();
                    
                    for (Column column : columns) {
                        String columnName = column.getName();
                        ColumnStore columnStore = tableData.getColumnStore(columnName);
                        
                        if (columnStore.isNull(i)) {
                            row.put(columnName, null);
                            continue;
                        }
                        
                        // Récupère la valeur en fonction du type
                        Object value = null;
                        try {
                            switch (column.getType()) {
                                case INTEGER:
                                    value = columnStore.getInt(i);
                                    break;
                                case LONG:
                                    value = columnStore.getLong(i);
                                    break;
                                case FLOAT:
                                    value = columnStore.getFloat(i);
                                    break;
                                case DOUBLE:
                                    value = columnStore.getDouble(i);
                                    break;
                                case BOOLEAN:
                                    value = columnStore.getBoolean(i);
                                    break;
                                case STRING:
                                    value = columnStore.getString(i);
                                    break;
                                case DATE:
                                case TIMESTAMP:
                                    value = columnStore.getDate(i);
                                    break;
                            }
                        } catch (Exception e) {
                            // En cas d'erreur, on met null et on continue
                            value = "ERROR: " + e.getMessage();
                        }
                        
                        row.put(columnName, value);
                    }
                    
                    rows.add(row);
                }
                
                // Construit la réponse
                Map<String, Object> result = new HashMap<>();
                result.put("tableName", tableName);
                result.put("totalRows", tableData.getRowCount());
                result.put("returnedRows", rows.size());
                result.put("data", rows);
                
                return Response.ok(result).build();
            } finally {
                tableData.readUnlock();
            }
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de l'accès aux données: " + e.getMessage())
                    .build();
        }
    }

    /**
     * Obtient des statistiques sur les données d'une table
     * @param tableName Le nom de la table
     * @return Les statistiques
     */
    @GET
    @Path("/{tableName}/stats")
    public Response getTableStats(@PathParam("tableName") String tableName) {
        try {
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            Table table = databaseContext.getTable(tableName);
            TableData tableData = databaseContext.getTableData(tableName);
            
            // Acquiert un verrou en lecture
            tableData.readLock();
            try {
                Map<String, Object> stats = new HashMap<>();
                stats.put("tableName", tableName);
                stats.put("rowCount", tableData.getRowCount());
                
                List<Map<String, Object>> columnStats = new ArrayList<>();
                for (Column column : table.getColumns()) {
                    Map<String, Object> colStat = new HashMap<>();
                    colStat.put("name", column.getName());
                    colStat.put("type", column.getType().name());
                    colStat.put("nullable", column.isNullable());
                    
                    // Analyse basique des données pour chaque colonne
                    ColumnStore columnStore = tableData.getColumnStore(column.getName());
                    int nullCount = 0;
                    
                    for (int i = 0; i < tableData.getRowCount(); i++) {
                        if (columnStore.isNull(i)) {
                            nullCount++;
                        }
                    }
                    
                    colStat.put("nullCount", nullCount);
                    colStat.put("nonNullCount", tableData.getRowCount() - nullCount);
                    
                    columnStats.add(colStat);
                }
                
                stats.put("columns", columnStats);
                
                return Response.ok(stats).build();
            } finally {
                tableData.readUnlock();
            }
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la récupération des statistiques: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Charge un fichier Parquet dans une table existante en mode distribué
     * Les données sont réparties entre les nœuds du cluster en round-robin
     * @param tableName Le nom de la table
     * @param payload Les informations pour le chargement distribué
     * @return La réponse HTTP
     */
    @POST
    @Path("/{tableName}/load-distributed")
    public Response loadDistributedParquet(
            @PathParam("tableName") String tableName,
            Map<String, Object> payload) {
        try {
            // Vérifie que le chemin du fichier est spécifié
            if (!payload.containsKey("filePath") || payload.get("filePath") == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le chemin du fichier est obligatoire")
                        .build();
            }
            
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            String filePath = payload.get("filePath").toString();
            
            // Options de chargement optimisées pour la mémoire
            ParquetLoadOptions options = new ParquetLoadOptions();
            
            // Paramètres optionnels
            if (payload.containsKey("batchSize")) {
                options.setBatchSize(((Number) payload.get("batchSize")).intValue());
            } else {
                // Taille de batch par défaut optimisée pour réduire la consommation mémoire
                options.setBatchSize(5000); 
            }
            
            if (payload.containsKey("rowLimit")) {
                options.setRowLimit(((Number) payload.get("rowLimit")).longValue());
            }
            
            if (payload.containsKey("timeoutSeconds")) {
                options.setTimeoutSeconds(((Number) payload.get("timeoutSeconds")).intValue());
            }
            
            // Force l'utilisation de l'accès direct pour bénéficier des optimisations mémoire
            options.setUseDirectAccess(true);
            
            // Parallelism pour optimiser le chargement
            if (payload.containsKey("parallelism")) {
                options.setParallelism(((Number) payload.get("parallelism")).intValue());
            }
            
            // Utilise le DistributedParquetLoader pour répartir les données entre les nœuds
            Map<String, Long> distributionStats;
            try {
                distributionStats = distributedParquetLoader.loadDistributed(tableName, filePath, options);
            } catch (Exception e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity("Erreur lors du chargement distribué: " + e.getMessage())
                        .build();
            }
            
            // Calcule le total des lignes chargées
            long totalRows = 0;
            for (Long count : distributionStats.values()) {
                totalRows += count;
            }
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("filePath", filePath);
            response.put("totalRowsLoaded", totalRows);
            response.put("distributionStats", distributionStats);
            response.put("message", "Fichier Parquet chargé avec succès en mode distribué");
            
            return Response.ok(response).build();
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors du chargement distribué: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Endpoint pour charger un segment spécifique d'un fichier Parquet
     * Utilisé par le mécanisme de distribution pour déléguer le chargement d'un segment à un nœud
     * 
     * @param tableName Le nom de la table
     * @param payload Les paramètres du segment à charger (filePath, startRow, rowCount, etc.)
     * @return La réponse HTTP avec le nombre de lignes chargées
     */
    @POST
    @Path("/{tableName}/load-parquet-segment")
    public Response loadParquetSegment(
            @PathParam("tableName") String tableName,
            Map<String, Object> payload) {
        try {
            // Vérifications des paramètres
            if (!payload.containsKey("filePath")) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le chemin du fichier est obligatoire")
                        .build();
            }
            
            // Vérifie les paramètres de filtrage modulo (nouvelle approche)
            if (!payload.containsKey("nodeIndex") || !payload.containsKey("nodeCount")) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Les paramètres de filtrage nodeIndex et nodeCount sont obligatoires")
                        .build();
            }
            
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            String filePath = payload.get("filePath").toString();
            
            // Récupère les paramètres de filtrage modulo
            int nodeIndex = ((Number) payload.get("nodeIndex")).intValue();
            int nodeCount = ((Number) payload.get("nodeCount")).intValue();
            
            logger.info("Chargement avec filtrage modulo: nodeIndex={}, nodeCount={} pour la table {}", 
                    nodeIndex, nodeCount, tableName);
            
            // Options optimisées pour la mémoire
            ParquetLoadOptions options = new ParquetLoadOptions();
            
            // Configure le filtrage modulo
            Map<String, Object> filterOptions = new HashMap<>();
            filterOptions.put("nodeIndex", nodeIndex);
            filterOptions.put("nodeCount", nodeCount);
            options.setFilterOptions(filterOptions);
            
            // Paramètres optionnels
            if (payload.containsKey("rowLimit")) {
                options.setRowLimit(((Number) payload.get("rowLimit")).longValue());
            }
            
            if (payload.containsKey("batchSize")) {
                options.setBatchSize(((Number) payload.get("batchSize")).intValue());
            }
            
            // Force l'utilisation des optimisations de mémoire
            options.setUseDirectAccess(true);
            
            if (payload.containsKey("parallelism")) {
                options.setParallelism(((Number) payload.get("parallelism")).intValue());
            }
            
            // Charge le segment de fichier avec les optimisations mémoire
            long loadedRows;
            try {
                // Utilise VectorizedParquetLoader qui supporte ParquetLoadOptions
                ParquetLoadStats stats = vectorizedParquetLoader.loadParquetFile(tableName, filePath, options);
                loadedRows = stats.getRowsProcessed();
            } catch (Exception e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity("Erreur lors du chargement du segment: " + e.getMessage())
                        .build();
            }
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("nodeIndex", nodeIndex);
            response.put("nodeCount", nodeCount);
            response.put("loadedRows", loadedRows);
            response.put("message", "Segment Parquet chargé avec succès par filtrage modulo");
            
            return Response.ok(response).build();
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors du chargement du segment: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Endpoint pour charger une ligne spécifique d'un fichier Parquet
     * Utilisé par le mécanisme de distribution pour répartir les lignes entre les nœuds
     * 
     * @param tableName Le nom de la table
     * @param payload Les paramètres (filePath, rowIndex)
     * @return La réponse HTTP indiquant si le chargement a réussi
     */
    @POST
    @Path("/{tableName}/load-specific-row")
    public Response loadSpecificRow(
            @PathParam("tableName") String tableName,
            Map<String, Object> payload) {
        try {
            // Vérifications des paramètres
            if (!payload.containsKey("filePath") || !payload.containsKey("rowIndex")) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Les paramètres filePath et rowIndex sont obligatoires")
                        .build();
            }
            
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            String filePath = payload.get("filePath").toString();
            int rowIndex = ((Number) payload.get("rowIndex")).intValue();
            
            logger.info("Chargement de la ligne {} du fichier {} dans la table {}",
                    rowIndex, filePath, tableName);
            
            // Charge la ligne spécifique 
            ParquetLoadStats stats;
            try {
                stats = vectorizedParquetLoader.loadSpecificRow(tableName, filePath, rowIndex);
            } catch (Exception e) {
                logger.error("Erreur lors du chargement de la ligne {}: {}", rowIndex, e.getMessage());
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity("Erreur lors du chargement de la ligne: " + e.getMessage())
                        .build();
            }
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("rowIndex", rowIndex);
            response.put("loaded", stats.getRowsProcessed() > 0);
            response.put("message", stats.getRowsProcessed() > 0 ?
                    "Ligne chargée avec succès" : "Ligne non trouvée ou déjà chargée");
            
            return Response.ok(response).build();
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors du chargement de la ligne: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Endpoint pour charger une plage de lignes d'un fichier Parquet
     * Optimisé pour les grands volumes de données en mode distribué
     * 
     * @param tableName Le nom de la table
     * @param payload Les paramètres (filePath, startRow, rowCount, batchSize)
     * @return La réponse HTTP indiquant si le chargement a réussi
     */
    @POST
    @Path("/{tableName}/load-range")
    public Response loadRange(
            @PathParam("tableName") String tableName,
            Map<String, Object> payload) {
        try {
            // Vérifications des paramètres
            if (!payload.containsKey("filePath") || !payload.containsKey("startRow") || 
                !payload.containsKey("rowCount")) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Les paramètres filePath, startRow et rowCount sont obligatoires")
                        .build();
            }
            
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            // Extraction des paramètres
            String filePath = payload.get("filePath").toString();
            int startRow = ((Number) payload.get("startRow")).intValue();
            int rowCount = ((Number) payload.get("rowCount")).intValue();
            int batchSize = payload.containsKey("batchSize") ? 
                    ((Number) payload.get("batchSize")).intValue() : 10000;
            
            logger.info("Chargement optimisé d'une plage de {} lignes à partir de l'index {} du fichier {}",
                    rowCount, startRow, filePath);
            
            // Prépare les options de chargement
            ParquetLoadOptions options = new ParquetLoadOptions();
            options.setBatchSize(batchSize);
            options.setSkipRows(startRow);
            options.setRowLimit(rowCount);
            
            // Charge les données
            ParquetLoadStats stats;
            try {
                long startTime = System.currentTimeMillis();
                stats = vectorizedParquetLoader.loadParquetFile(tableName, filePath, options);
                long duration = System.currentTimeMillis() - startTime;
                
                logger.info("Plage de lignes chargée avec succès: {} lignes en {} ms", 
                        stats.getRowsProcessed(), duration);
            } catch (Exception e) {
                logger.error("Erreur lors du chargement de la plage de lignes: {}", e.getMessage());
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity("Erreur lors du chargement de la plage: " + e.getMessage())
                        .build();
            }
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("startRow", startRow);
            response.put("requestedCount", rowCount);
            response.put("loadedRows", stats.getRowsProcessed());
            response.put("elapsedMs", stats.getElapsedTimeMs());
            response.put("message", "Plage de lignes chargée avec succès");
            
            return Response.ok(response).build();
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors du chargement de la plage: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Endpoint pour charger un fichier Parquet via multipart upload et le distribuer entre les nœuds
     * Cette méthode permet de télécharger le fichier Parquet sur le serveur puis de le distribuer
     * @param tableName Le nom de la table
     * @param form Le formulaire multipart contenant le fichier et les options
     * @return La réponse HTTP indiquant si le chargement a réussi
     */
    @POST
    @Path("/{tableName}/load-distributed-upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response loadDistributedWithUpload(
            @PathParam("tableName") String tableName,
            @MultipartForm DistributedParquetUploadForm form) {
        try {
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            if (form.file == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le fichier Parquet est obligatoire")
                        .build();
            }
            
            // Récupération du fichier temporaire
            File tempFile = saveToTempFile(form.file);
            logger.info("Fichier téléchargé et sauvegardé temporairement: {}", tempFile.getAbsolutePath());
            
            // Options de chargement
            ParquetLoadOptions options = new ParquetLoadOptions();
            options.setRowLimit(form.rowLimit);
            options.setBatchSize(form.batchSize);
            
            // Valider le schéma du fichier Parquet par rapport à la table
            try {
                // Vérification simple que le fichier existe et peut être lu
                if (!new File(tempFile.getAbsolutePath()).exists()) {
                    throw new IOException("Fichier temporaire non trouvé: " + tempFile.getAbsolutePath());
                }
                
                // Note: Dans une implémentation complète, nous devrions vérifier la compatibilité du schéma ici
                logger.info("Fichier Parquet validé avec succès");
            } catch (Exception e) {
                logger.error("Erreur lors de la validation du fichier Parquet: {}", e.getMessage());
                tempFile.delete();
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le fichier Parquet n'est pas compatible avec la table: " + e.getMessage())
                        .build();
            }
            
            // Chargement distribué (avec propagation du fichier)
            Map<String, Long> distributionStats;
            long startTime = System.currentTimeMillis();
            try {
                // Utilisation du chargeur distribué
                distributionStats = distributedParquetLoader.loadDistributed(tableName, tempFile.getAbsolutePath(), options);
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Fichier distribué et chargé avec succès en {} ms", duration);
            } catch (Exception e) {
                logger.error("Erreur lors du chargement distribué: {}", e.getMessage());
                tempFile.delete();
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity("Erreur lors du chargement distribué: " + e.getMessage())
                        .build();
            }
            
            // Supprime le fichier temporaire après utilisation
            boolean deleted = tempFile.delete();
            if (!deleted) {
                logger.warn("Impossible de supprimer le fichier temporaire: {}", tempFile.getAbsolutePath());
            }
            
            // Construction de la réponse
            Map<String, Object> result = new HashMap<>();
            result.put("tableName", tableName);
            result.put("distributionStats", distributionStats);
            result.put("totalRowsLoaded", 
                    distributionStats.values().stream().mapToLong(Long::longValue).sum());
            result.put("message", "Fichier Parquet téléchargé et chargé avec succès en mode distribué");
            result.put("elapsedMs", System.currentTimeMillis() - startTime);
            
            return Response.ok(result).build();
        } catch (Exception e) {
            logger.error("Erreur lors du chargement distribué avec upload: {}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Endpoint pour charger un fichier Parquet via son chemin et le distribuer entre les nœuds
     * Compatible avec l'ancien format JSON qui spécifie un chemin de fichier local
     * @param tableName Le nom de la table
     * @param payload Les informations pour le chargement distribué
     * @return La réponse HTTP indiquant si le chargement a réussi
     */
    @POST
    @Path("/{tableName}/load-distributed-upload")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response loadDistributedWithPath(
            @PathParam("tableName") String tableName,
            ParquetLoadRequest payload) {
        try {
            // Vérifie que la table existe
            if (!databaseContext.tableExists(tableName)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Table inconnue: " + tableName)
                        .build();
            }
            
            if (payload.filePath == null && payload.file == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le chemin du fichier Parquet est obligatoire (filePath ou file)")
                        .build();
            }
            
            // Utilisation du champ filePath ou file (pour compatibilité)
            String filePath = payload.filePath != null ? payload.filePath : payload.file;
            
            // Vérifier que le fichier existe
            File file = new File(filePath);
            if (!file.exists() || !file.isFile()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le fichier Parquet n'existe pas: " + filePath)
                        .build();
            }
            
            // Options de chargement
            ParquetLoadOptions options = new ParquetLoadOptions();
            options.setRowLimit(payload.rowLimit);
            options.setBatchSize(payload.batchSize);
            
            // Chargement distribué
            Map<String, Long> distributionStats;
            long startTime = System.currentTimeMillis();
            try {
                // Utilisation du chargeur distribué
                distributionStats = distributedParquetLoader.loadDistributed(tableName, filePath, options);
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Fichier distribué et chargé avec succès en {} ms", duration);
            } catch (Exception e) {
                logger.error("Erreur lors du chargement distribué: {}", e.getMessage());
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity("Erreur lors du chargement distribué: " + e.getMessage())
                        .build();
            }
            
            // Construction de la réponse
            Map<String, Object> result = new HashMap<>();
            result.put("tableName", tableName);
            result.put("distributionStats", distributionStats);
            result.put("totalRowsLoaded", 
                    distributionStats.values().stream().mapToLong(Long::longValue).sum());
            result.put("message", "Fichier Parquet chargé avec succès en mode distribué");
            result.put("elapsedMs", System.currentTimeMillis() - startTime);
            
            return Response.ok(result).build();
        } catch (Exception e) {
            logger.error("Erreur lors du chargement distribué avec un chemin: {}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Méthode utilitaire pour sauvegarder le fichier téléchargé
     * @param inputStream Le flux d'entrée du fichier
     * @return Le fichier temporaire créé
     */
    private File saveToTempFile(InputStream inputStream) throws IOException {
        File tempFile = File.createTempFile("parquet_upload_", ".parquet");
        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        return tempFile;
    }

    /**
     * Classe de requête pour le chargement de fichiers Parquet
     */
    static class ParquetLoadRequest {
        public String filePath;
        public String file; // Alias pour filePath (pour compatibilité)
        public long rowLimit = -1;
        public int batchSize = 100000;
    }
}