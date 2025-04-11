package com.memorydb.rest;

import com.memorydb.common.DataType;
import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.distribution.ClusterManager;
import com.memorydb.parquet.ParquetLoader;
import com.memorydb.rest.dto.ColumnDto;
import com.memorydb.rest.dto.TableDto;
import com.memorydb.storage.ColumnStore;
import com.memorydb.storage.TableData;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
    
    @Inject
    private DatabaseContext databaseContext;
    
    @Inject
    private ClusterManager clusterManager;
    
    @Inject
    private ParquetLoader parquetLoader;
    
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
} 