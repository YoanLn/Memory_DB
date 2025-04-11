package com.memorydb.rest;

import com.memorydb.parquet.ParquetSchemaReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resource REST pour les opérations liées aux fichiers Parquet
 */
@Path("/api/parquet")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class ParquetResource {
    
    /**
     * Lit et affiche le schéma d'un fichier Parquet
     * @param filePath Le chemin du fichier Parquet
     * @return Les informations sur le schéma
     */
    @GET
    @Path("/schema")
    public Response getParquetSchema(@QueryParam("filePath") String filePath) {
        try {
            if (filePath == null || filePath.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Le paramètre 'filePath' est obligatoire")
                        .build();
            }
            
            MessageType schema = ParquetSchemaReader.readSchema(filePath);
            
            if (schema == null) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity("Impossible de lire le schéma du fichier")
                        .build();
            }
            
            // Construit la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("filePath", filePath);
            response.put("schemaName", schema.getName());
            
            List<Map<String, String>> fields = new ArrayList<>();
            for (Type field : schema.getFields()) {
                Map<String, String> fieldInfo = new HashMap<>();
                fieldInfo.put("name", field.getName());
                fieldInfo.put("type", field.asPrimitiveType().getPrimitiveTypeName().name());
                fieldInfo.put("repetition", field.getRepetition().name());
                fields.add(fieldInfo);
            }
            
            response.put("fields", fields);
            
            return Response.ok(response).build();
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur lors de la lecture du schéma: " + e.getMessage())
                    .build();
        }
    }
} 