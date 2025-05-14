package com.memorydb.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.jboss.resteasy.annotations.providers.multipart.MultipartForm;
import org.jboss.resteasy.annotations.providers.multipart.PartType;

/**
 * Gestionnaire des ressources pour les transferts de fichiers entre nœuds
 * Permet la vérification de l'existence et le téléchargement des fichiers
 */
@javax.ws.rs.Path("/api/files")
@ApplicationScoped
public class FileResource {
    
    private static final Logger logger = LoggerFactory.getLogger(FileResource.class);
    
    // Répertoire pour les fichiers temporaires partagés
    private static final String SHARED_FILES_DIR = System.getProperty("java.io.tmpdir") + "/memorydb-shared";
    
    /**
     * Constructeur: crée le répertoire partagé s'il n'existe pas
     */
    public FileResource() {
        try {
            Path sharedDir = Paths.get(SHARED_FILES_DIR);
            if (!Files.exists(sharedDir)) {
                Files.createDirectories(sharedDir);
                logger.info("Répertoire pour fichiers partagés créé: {}", SHARED_FILES_DIR);
            }
        } catch (IOException e) {
            logger.error("Impossible de créer le répertoire pour les fichiers partagés: {}", e.getMessage());
        }
    }
    
    /**
     * Vérifie si un fichier existe sur ce nœud
     * @param path Chemin du fichier à vérifier
     * @return Réponse indiquant si le fichier existe
     */
    @GET
    @javax.ws.rs.Path("/check")
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkFileExists(@QueryParam("path") String path) {
        logger.info("Vérification de l'existence du fichier: {}", path);
        
        Map<String, Object> result = new HashMap<>();
        File file = new File(path);
        
        // Vérifie aussi dans le répertoire partagé si le fichier n'est pas un chemin absolu
        boolean exists = file.exists();
        if (!exists && !file.isAbsolute()) {
            file = new File(SHARED_FILES_DIR, file.getName());
            exists = file.exists();
        }
        
        result.put("exists", exists);
        result.put("path", file.getAbsolutePath());
        
        if (exists) {
            result.put("size", file.length());
            result.put("lastModified", file.lastModified());
        }
        
        return Response.ok(result).build();
    }
    
    /**
     * Classe pour gérer l'upload de fichier via multipart form
     */
    public static class FileUploadForm {
        @FormParam("file")
        @PartType(MediaType.APPLICATION_OCTET_STREAM)
        public InputStream file;
        
        @FormParam("filename")
        @PartType(MediaType.TEXT_PLAIN)
        public String filename;
        
        @FormParam("originalPath")
        @PartType(MediaType.TEXT_PLAIN)
        public String originalPath;
    }
    
    /**
     * Reçoit un fichier d'un autre nœud et le sauvegarde
     * @param form Formulaire multipart contenant le fichier
     * @return Réponse avec le chemin où le fichier a été sauvegardé
     */
    @POST
    @javax.ws.rs.Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response uploadFile(@MultipartForm FileUploadForm form) {
        try {
            if (form.file == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Aucun fichier n'a été envoyé").build();
            }
            
            String filename = form.filename;
            if (filename == null || filename.isEmpty()) {
                filename = "temp-" + UUID.randomUUID() + ".parquet";
            }
            
            // Sauvegarde le fichier dans le répertoire partagé
            Path targetPath = Paths.get(SHARED_FILES_DIR, filename);
            Files.copy(form.file, targetPath, StandardCopyOption.REPLACE_EXISTING);
            
            logger.info("Fichier reçu et sauvegardé: {}", targetPath);
            
            // Construit la réponse
            Map<String, String> result = new HashMap<>();
            result.put("path", targetPath.toString());
            result.put("originalPath", form.originalPath);
            
            return Response.ok(result).build();
        } catch (Exception e) {
            logger.error("Erreur lors de la réception du fichier: {}", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur: " + e.getMessage()).build();
        }
    }
    
    /**
     * Récupère un fichier stocké sur ce nœud
     * @param path Chemin du fichier à récupérer
     * @return Le fichier en tant que flux d'octets
     */
    @GET
    @javax.ws.rs.Path("/download")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response downloadFile(@QueryParam("path") String path) {
        try {
            File file = new File(path);
            
            // Vérifie aussi dans le répertoire partagé si le fichier n'est pas trouvé
            if (!file.exists() && !file.isAbsolute()) {
                file = new File(SHARED_FILES_DIR, file.getName());
            }
            
            if (!file.exists()) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Fichier non trouvé: " + path)
                        .build();
            }
            
            String filename = file.getName();
            
            return Response.ok(file)
                    .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                    .build();
        } catch (Exception e) {
            logger.error("Erreur lors du téléchargement du fichier: {}", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Erreur: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Renvoie le chemin absolu d'un fichier dans le répertoire partagé
     * Utile pour la standardisation des chemins entre nœuds
     * @param filename Nom du fichier
     * @return Chemin absolu dans le répertoire partagé
     */
    public static String getSharedFilePath(String filename) {
        if (filename == null || filename.isEmpty()) {
            return null;
        }
        
        File file = new File(filename);
        if (file.isAbsolute()) {
            return filename;
        }
        
        return Paths.get(SHARED_FILES_DIR, file.getName()).toString();
    }
}
