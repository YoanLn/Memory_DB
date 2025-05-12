package com.memorydb.rest;

import java.io.InputStream;

import javax.ws.rs.FormParam;
import javax.ws.rs.core.MediaType;

/**
 * Formulaire pour le téléchargement et le chargement distribué de fichiers Parquet
 */
public class DistributedParquetUploadForm {
    
    @FormParam("file")
    public InputStream file;
    
    @FormParam("rowLimit")
    public long rowLimit = -1;
    
    @FormParam("batchSize")
    public int batchSize = 100000;
}
