package com.memorydb.rest;

import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.storage.ColumnStore;
import com.memorydb.storage.TableData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Resource REST pour l'export des données
 */
@Path("/api/export")
@ApplicationScoped
public class ExportResource {

    private static final Logger logger = LoggerFactory.getLogger(ExportResource.class);
    private static final String CSV_CONTENT_TYPE = "text/csv";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final char CSV_DELIMITER = ',';
    private static final char CSV_QUOTE = '"';
    private static final String CSV_LINE_END = "\n";
    private static final int BATCH_SIZE = 1000; // Nombre de lignes à traiter par lot

    @Inject
    private DatabaseContext databaseContext;

    /**
     * Exporte les données d'une table au format CSV
     * @param tableName Le nom de la table
     * @param limit Limite optionnelle du nombre de lignes
     * @param offset Offset optionnel pour la pagination
     * @param filter Filtre optionnel au format JSON
     * @return La réponse HTTP avec les données CSV
     */
    @GET
    @Path("/{tableName}/csv")
    @Produces(CSV_CONTENT_TYPE)
    public Response exportToCsv(
            @PathParam("tableName") String tableName,
            @QueryParam("limit") @DefaultValue("-1") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset,
            @QueryParam("filter") String filter) {
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
            
            // Streaming output pour éviter de charger toutes les données en mémoire
            StreamingOutput stream = output -> {
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(output, StandardCharsets.UTF_8));
                
                try {
                    // En-tête CSV
                    List<String> headerRow = new ArrayList<>();
                    for (Column column : columns) {
                        headerRow.add(column.getName());
                    }
                    writeCSVRow(writer, headerRow);
                    
                    // Calcul des bornes
                    tableData.readLock();
                    try {
                        int rowCount = tableData.getRowCount();
                        int endRow = (limit > 0) ? Math.min(offset + limit, rowCount) : rowCount;

                        // Export des données par lots
                        for (int i = offset; i < endRow; i++) {
                            List<String> dataRow = new ArrayList<>();
                            
                            for (Column column : columns) {
                                ColumnStore columnStore = tableData.getColumnStore(column.getName());
                                Object value = extractValue(i, columnStore);
                                dataRow.add(value == null ? "" : value.toString());
                            }
                            
                            writeCSVRow(writer, dataRow);
                            
                            // Flush tous les BATCH_SIZE lignes pour éviter d'utiliser trop de mémoire
                            if (i % BATCH_SIZE == 0) {
                                writer.flush();
                            }
                        }
                    } finally {
                        tableData.readUnlock();
                    }
                    
                    writer.flush();
                } catch (Exception e) {
                    logger.error("Erreur lors de l'export CSV: {}", e.getMessage(), e);
                    writer.write("Erreur lors de l'export: " + e.getMessage());
                }
            };
            
            String filename = tableName + ".csv";
            return Response.ok(stream)
                    .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                    .build();
            
        } catch (Exception e) {
            logger.error("Erreur lors de l'export CSV: {}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de l'export: " + e.getMessage())
                    .build();
        }
    }

    /**
     * Exporte les données d'une table au format JSON
     * @param tableName Le nom de la table
     * @param limit Limite optionnelle du nombre de lignes
     * @param offset Offset optionnel pour la pagination
     * @param filter Filtre optionnel au format JSON
     * @return La réponse HTTP avec les données JSON
     */
    @GET
    @Path("/{tableName}/json")
    @Produces(MediaType.APPLICATION_JSON)
    public Response exportToJson(
            @PathParam("tableName") String tableName,
            @QueryParam("limit") @DefaultValue("-1") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset,
            @QueryParam("filter") String filter) {
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
            
            // Streaming output pour éviter de charger toutes les données en mémoire
            StreamingOutput stream = output -> {
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(output, StandardCharsets.UTF_8));
                
                try {
                    tableData.readLock();
                    try {
                        writer.write("[");
                        
                        int rowCount = tableData.getRowCount();
                        int endRow = (limit > 0) ? Math.min(offset + limit, rowCount) : rowCount;
                        boolean firstRow = true;
                        
                        // Export des données par lots
                        for (int i = offset; i < endRow; i++) {
                            if (!firstRow) {
                                writer.write(",");
                            }
                            firstRow = false;
                            
                            writer.write("\n  {");
                            boolean firstColumn = true;
                            
                            for (Column column : columns) {
                                if (!firstColumn) {
                                    writer.write(",");
                                }
                                firstColumn = false;
                                
                                String columnName = column.getName();
                                ColumnStore columnStore = tableData.getColumnStore(columnName);
                                Object value = extractValue(i, columnStore);
                                
                                writer.write("\n    \"" + escapeJsonString(columnName) + "\": ");
                                if (value == null) {
                                    writer.write("null");
                                } else if (value instanceof Number || value instanceof Boolean) {
                                    writer.write(value.toString());
                                } else {
                                    writer.write("\"" + escapeJsonString(value.toString()) + "\"");
                                }
                            }
                            
                            writer.write("\n  }");
                            
                            // Flush tous les BATCH_SIZE lignes pour éviter d'utiliser trop de mémoire
                            if (i % BATCH_SIZE == 0) {
                                writer.flush();
                            }
                        }
                        
                        writer.write("\n]");
                    } finally {
                        tableData.readUnlock();
                    }
                    
                    writer.flush();
                } catch (Exception e) {
                    logger.error("Erreur lors de l'export JSON: {}", e.getMessage(), e);
                    writer.write("{\"error\": \"" + escapeJsonString(e.getMessage()) + "\"}");
                }
            };
            
            String filename = tableName + ".json";
            return Response.ok(stream)
                    .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                    .build();
            
        } catch (Exception e) {
            logger.error("Erreur lors de l'export JSON: {}", e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de l'export: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Écrit une ligne CSV en échappant les caractères spéciaux
     * @param writer Le writer
     * @param values Les valeurs à écrire
     * @throws IOException Si une erreur survient lors de l'écriture
     */
    private void writeCSVRow(BufferedWriter writer, List<String> values) throws IOException {
        boolean first = true;
        
        for (String value : values) {
            if (!first) {
                writer.write(CSV_DELIMITER);
            }
            first = false;
            
            // Échappement des valeurs CSV
            if (value != null) {
                boolean needsQuoting = value.contains(String.valueOf(CSV_DELIMITER)) 
                    || value.contains(String.valueOf(CSV_QUOTE))
                    || value.contains("\n")
                    || value.contains("\r");
                
                if (needsQuoting) {
                    writer.write(CSV_QUOTE);
                    writer.write(value.replace(String.valueOf(CSV_QUOTE), 
                        String.valueOf(CSV_QUOTE) + CSV_QUOTE));
                    writer.write(CSV_QUOTE);
                } else {
                    writer.write(value);
                }
            }
        }
        
        writer.write(CSV_LINE_END);
    }
    
    /**
     * Échappe une chaîne pour le format JSON
     * @param input La chaîne à échapper
     * @return La chaîne échappée
     */
    private String escapeJsonString(String input) {
        if (input == null) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            switch (ch) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (ch < ' ') {
                        String hex = String.format("\\u%04x", (int) ch);
                        sb.append(hex);
                    } else {
                        sb.append(ch);
                    }
            }
        }
        
        return sb.toString();
    }
    
    /**
     * Extrait la valeur d'une cellule en fonction du type de colonne
     * @param rowIndex L'index de la ligne
     * @param columnStore Le ColumnStore
     * @return La valeur
     */
    private Object extractValue(int rowIndex, ColumnStore columnStore) {
        if (columnStore.isNull(rowIndex)) {
            return null;
        }
        
        switch (columnStore.getType()) {
            case INTEGER:
                return columnStore.getInt(rowIndex);
            case LONG:
                return columnStore.getLong(rowIndex);
            case FLOAT:
                return columnStore.getFloat(rowIndex);
            case DOUBLE:
                return columnStore.getDouble(rowIndex);
            case BOOLEAN:
                return columnStore.getBoolean(rowIndex);
            case STRING:
                return columnStore.getString(rowIndex);
            case DATE:
            case TIMESTAMP:
                return columnStore.getDate(rowIndex);
            default:
                return null;
        }
    }
} 