package com.memorydb.rest;

import java.io.InputStream;

import org.jboss.resteasy.annotations.providers.multipart.PartType;

import javax.ws.rs.FormParam;
import javax.ws.rs.core.MediaType;

/**
 * Formulaire pour le téléchargement et le chargement distribué de fichiers Parquet
 */
public class DistributedParquetUploadForm {
    
    @FormParam("file")
    @PartType(MediaType.APPLICATION_OCTET_STREAM)
    public InputStream file;
    
    @FormParam("rowLimit")
    @PartType(MediaType.TEXT_PLAIN)
    public long rowLimit = -1;
    
    @FormParam("batchSize")
    @PartType(MediaType.TEXT_PLAIN)
    public int batchSize = 100000;
}
