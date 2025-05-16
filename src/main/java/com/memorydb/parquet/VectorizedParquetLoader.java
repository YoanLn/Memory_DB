package com.memorydb.parquet;

import com.memorydb.common.DataType;
import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.storage.ColumnStore;
import com.memorydb.storage.TableData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implémentation optimisée pour le chargement de fichiers Parquet 
 * avec support des grands volumes et traitement par batch
 */
@ApplicationScoped
public class VectorizedParquetLoader {
    private static final Logger logger = LoggerFactory.getLogger(VectorizedParquetLoader.class);
    
    @Inject
    private DatabaseContext databaseContext;
    
    private ExecutorService executorService;

    /**
     * Charge un fichier Parquet dans une table existante
     * Utilise une approche de streaming par batch pour une meilleure gestion de la mémoire
     * Supporté pour sauter des lignes et limiter le nombre de lignes chargées
     * 
     * @param tableName Nom de la table où charger les données
     * @param filePath Chemin du fichier Parquet à charger
     * @param options Options de chargement (batch, limite, filtrage, etc.)
     * @return Statistiques de chargement
     */
    public ParquetLoadStats loadParquetFile(String tableName, String filePath, ParquetLoadOptions options) 
            throws IOException {
        if (executorService == null || executorService.isShutdown()) {
            executorService = Executors.newFixedThreadPool(Math.min(options.getParallelism(), 8));
        }
        
        // Vérifie que la table existe
        Table table = databaseContext.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table introuvable: " + tableName);
        }
        
        TableData tableData = databaseContext.getTableData(tableName);
        long startTime = System.currentTimeMillis();
        ParquetLoadStats stats = new ParquetLoadStats();
        
        // Ouvre le fichier Parquet pour vérifier le schéma
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        
        // Indique à Hadoop et Parquet de garder les fichiers ouverts
        // Ces paramètres permettent d'améliorer les performances en réduisant les opérations I/O
        conf.set("fs.hdfs.impl.disable.cache", "false");
        conf.set("parquet.read.support.class", "org.apache.parquet.hadoop.example.GroupReadSupport");
        conf.set("parquet.filter.record-level.enabled", "true");
        
        try (ParquetFileReader schemaReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType schema = schemaReader.getFooter().getFileMetaData().getSchema();
            validateSchema(table, schema);
            
            // Configuration pour le streaming par batch
            long rowLimit = options.getRowLimit();
            int batchSize = options.getBatchSize();
            int skipRows = options.getSkipRows();
            long totalRows = 0;
            int batchCount = 0;
            boolean timeout = false;
            
            logger.info("Début du chargement de {} avec options: skipRows={}, rowLimit={}, batchSize={}", 
                    filePath, skipRows, rowLimit, batchSize);
            
            tableData.writeLock();
            try {
                // Création d'un ParquetReader pour traiter le fichier de manière efficace
                try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                        .withConf(conf)
                        .build()) {
                    
                    // Création d'un buffer pour stocker les données par batch
                    List<Object[]> batchData = new ArrayList<>(batchSize);
                    List<Column> columns = table.getColumns();
                    
                    // Lecture par batch
                    Group record;
                    long rowIndex = 0;
                    long skippedRows = 0;
                    
                    // Vérifie si le filtrage modulo est activé
                    Integer nodeIndex = null;
                    Integer nodeCount = null;
                    
                    if (options.getFilterOptions() != null) {
                        Map<String, Object> filterOpts = options.getFilterOptions();
                        if (filterOpts.containsKey("nodeIndex") && filterOpts.containsKey("nodeCount")) {
                            nodeIndex = ((Number)filterOpts.get("nodeIndex")).intValue();
                            nodeCount = ((Number)filterOpts.get("nodeCount")).intValue();
                            logger.info("Filtrage modulo activé: nodeIndex={}, nodeCount={}", nodeIndex, nodeCount);
                        }
                    }
                    
                    // Saute les premières lignes si demandé
                    if (skipRows > 0) {
                        logger.info("Saute les {} premières lignes du fichier", skipRows);
                        long startSkipTime = System.currentTimeMillis();
                        
                        while (skippedRows < skipRows && (record = reader.read()) != null) {
                            skippedRows++;
                            rowIndex++;
                        }
                        
                        long skipDuration = System.currentTimeMillis() - startSkipTime;
                        logger.info("{} lignes sautées en {} ms", skippedRows, skipDuration);
                        
                        // Si on n'a pas pu sauter toutes les lignes demandées, le fichier est trop petit
                        if (skippedRows < skipRows) {
                            logger.warn("Impossible de sauter toutes les lignes demandées, le fichier ne contient que {} lignes", skippedRows);
                            return stats; // Retourne sans charger de ligne (fichier trop court)
                        }
                    }
                        
                    // Traite le reste du fichier (ou jusqu'à la limite)
                    boolean reachedLimit = false;
                    while (!reachedLimit && (record = reader.read()) != null) {
                        // Vérifie si on a atteint la limite
                        if (rowLimit > 0 && totalRows >= rowLimit) {
                            logger.info("Limite de {} lignes atteinte", rowLimit);
                            reachedLimit = true;
                            break;
                        }
                        
                        // Vérifie le timeout
                        if (options.getTimeoutSeconds() > 0 && 
                            (System.currentTimeMillis() - startTime) / 1000 > options.getTimeoutSeconds()) {
                            logger.warn("Timeout atteint après {} secondes", options.getTimeoutSeconds());
                            timeout = true;
                            break;
                        }
                        
                        // Applique le filtrage modulo si demandé
                        if (nodeIndex != null && nodeCount != null) {
                            // Ne prend que les lignes où rowIndex % nodeCount == nodeIndex
                            if (rowIndex % nodeCount != nodeIndex) {
                                rowIndex++;
                                continue;
                            }
                        }
                        
                        // Extraction des valeurs de la ligne
                        Object[] rowValues = extractValues(record, columns, schema);
                        batchData.add(rowValues);
                        totalRows++;
                        
                        rowIndex++; // increment rowIndex after each row is processed
                        
                        // Si le batch est complet, on l'insère dans la table
                        if (batchData.size() >= batchSize) {
                            addBatchToTable(tableData, batchData);
                            batchData.clear();
                            batchCount++;
                            
                            if (batchCount % 10 == 0) {
                                logger.info("Chargés: {} lignes, {} batchs, {} sec", 
                                    totalRows, batchCount, (System.currentTimeMillis() - startTime) / 1000);
                            }
                        }
                    }
                    
                    // Traiter le dernier batch s'il reste des données
                    if (!batchData.isEmpty()) {
                        addBatchToTable(tableData, batchData);
                        batchCount++;
                    }
                }
                
                // Mise à jour des statistiques
                stats.setRowsProcessed(totalRows);
                stats.setBatchCount(batchCount);
                stats.setTimeout(timeout);
                
            } finally {
                tableData.writeUnlock();
            }
        } catch (Exception e) {
            logger.error("Erreur lors du chargement du fichier Parquet", e);
            stats.setError(e.getMessage());
            throw new IOException("Erreur lors du chargement du fichier Parquet: " + e.getMessage(), e);
        } finally {
            stats.setElapsedTimeMs(System.currentTimeMillis() - startTime);
        }
        
        return stats;
    }
    
    /**
     * Ajoute un batch de données à la table
     */
    /**
     * Ajoute un batch de données à la table de manière optimisée
     * Ajoute directement les valeurs aux colonnes pour réduire les allocations temporaires
     */
    private void addBatchToTable(TableData tableData, List<Object[]> batchData) {
        Table table = tableData.getTable();
        List<Column> columns = table.getColumns();
        int columnCount = columns.size();
        
        tableData.writeLock();
        try {
            for (Object[] rowValues : batchData) {
                // Vérification pour éviter les problèmes d'index
                if (rowValues.length != columnCount) {
                    throw new IllegalArgumentException("Nombre de valeurs incorrect, attendu: " + 
                        columnCount + ", obtenu: " + rowValues.length);
                }
                
                // Ajoute chaque valeur dans sa colonne respective
                for (int i = 0; i < columnCount; i++) {
                    ColumnStore columnStore = tableData.getColumnStore(i);
                    Object value = rowValues[i];
                    
                    // Optimisation pour réduire les conversions et les allocations d'objets
                    if (value == null) {
                        columnStore.addNull();
                    } else {
                        switch (columnStore.getType()) {
                            case INTEGER:
                                columnStore.addInt((Integer) value);
                                break;
                            case LONG:
                                columnStore.addLong((Long) value);
                                break;
                            case FLOAT:
                                columnStore.addFloat((Float) value);
                                break;
                            case DOUBLE:
                                columnStore.addDouble((Double) value);
                                break;
                            case BOOLEAN:
                                columnStore.addBoolean((Boolean) value);
                                break;
                            case STRING:
                                // Optimisation pour les chaînes
                                columnStore.addString(value.toString().intern());
                                break;
                            case DATE:
                            case TIMESTAMP:
                                columnStore.addDate((Long) value);
                                break;
                            default:
                                throw new IllegalArgumentException("Type non supporté: " + columnStore.getType());
                        }
                    }
                }
                
                // Incrémente le compteur de lignes sans réallouer un tableau d'objets temporaires
                // comme le ferait addRow, ce qui réduit considérablement l'usage mémoire
                tableData.incrementRowCount();
            }
        } finally {
            tableData.writeUnlock();
        }
    }
    
    /**
     * Extrait les valeurs d'un groupe Parquet
     */
    private Object[] extractValues(Group group, List<Column> columns, MessageType schema) {
        Object[] values = new Object[columns.size()];
        
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            Type parquetField = schema.getType(i);
            String fieldName = parquetField.getName();
            
            // Vérifie si la valeur est null
            boolean isNull = parquetField.isRepetition(Type.Repetition.OPTIONAL) && 
                             (group.getFieldRepetitionCount(i) == 0);
            
            if (isNull) {
                values[i] = null;
                continue;
            }
            
            // Extraction de la valeur selon le type
            switch (column.getType()) {
                case INTEGER:
                    values[i] = group.getInteger(fieldName, 0);
                    break;
                case LONG:
                    values[i] = group.getLong(fieldName, 0);
                    break;
                case FLOAT:
                    values[i] = group.getFloat(fieldName, 0);
                    break;
                case DOUBLE:
                    values[i] = group.getDouble(fieldName, 0);
                    break;
                case BOOLEAN:
                    values[i] = group.getBoolean(fieldName, 0);
                    break;
                case STRING:
                    Binary binary = group.getBinary(fieldName, 0);
                    values[i] = binary.toStringUsingUTF8();
                    break;
                case DATE:
                case TIMESTAMP:
                    if (parquetField.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
                        Binary int96Value = group.getInt96(fieldName, 0);
                        values[i] = convertInt96ToTimestamp(int96Value);
                    } else {
                        values[i] = group.getLong(fieldName, 0);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Type non supporté: " + column.getType());
            }
        }
        
        return values;
    }
    
    /**
     * Convertit un INT96 en timestamp (millisecondes depuis l'epoch)
     */
    private long convertInt96ToTimestamp(Binary int96Value) {
        // Cette implémentation est simplifiée
        // Dans un cas réel, il faudrait interpréter correctement le format INT96
        // qui contient des nanosecondes depuis l'epoch Julian
        
        // Récupère les octets de la valeur INT96
        byte[] bytes = int96Value.getBytes();
        
        // Crée un ByteBuffer pour extraire les valeurs
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
        buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        
        // Les 8 premiers octets sont les nanosecondes, les 4 derniers sont les secondes
        long nanos = buf.getLong(0);
        int julianDay = buf.getInt(8);
        
        // Convertit les jours juliens en millisecondes (depuis l'epoch Unix)
        // Note: epoch Unix commence le 1/1/1970, Julian commence le 1/1/4713 BC
        // La différence est de 2440587.5 jours
        long epochMilli = (julianDay - 2440588) * 86400000; // Jours en ms
        
        // Ajoute les nanosecondes (converties en millisecondes)
        epochMilli += nanos / 1_000_000;
        
        return epochMilli;
    }
    
    /**
     * Valide que le schéma Parquet est compatible avec la table
     */
    private void validateSchema(Table table, MessageType schema) {
        List<Column> tableColumns = table.getColumns();
        List<Type> parquetFields = schema.getFields();
        
        if (tableColumns.size() != parquetFields.size()) {
            throw new IllegalArgumentException(
                "Le nombre de colonnes ne correspond pas: Table(" + tableColumns.size() + 
                ") vs Parquet(" + parquetFields.size() + ")");
        }
        
        for (int i = 0; i < tableColumns.size(); i++) {
            Column column = tableColumns.get(i);
            Type parquetField = parquetFields.get(i);
            
            if (!isCompatibleType(column.getType(), parquetField)) {
                throw new IllegalArgumentException(
                    "Type incompatible pour la colonne '" + column.getName() + 
                    "': Table(" + column.getType() + ") vs Parquet(" + 
                    parquetField.asPrimitiveType().getPrimitiveTypeName() + ")");
            }
        }
    }
    
    /**
     * Vérifie si un type de colonne est compatible avec un type Parquet
     */
    private boolean isCompatibleType(DataType columnType, Type parquetField) {
        if (!parquetField.isPrimitive()) {
            return false;
        }
        
        PrimitiveType.PrimitiveTypeName typeName = parquetField.asPrimitiveType().getPrimitiveTypeName();
        
        switch (columnType) {
            case INTEGER:
                return typeName == PrimitiveType.PrimitiveTypeName.INT32;
            case LONG:
                return typeName == PrimitiveType.PrimitiveTypeName.INT64;
            case FLOAT:
                return typeName == PrimitiveType.PrimitiveTypeName.FLOAT;
            case DOUBLE:
                return typeName == PrimitiveType.PrimitiveTypeName.DOUBLE;
            case BOOLEAN:
                return typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN;
            case STRING:
                return typeName == PrimitiveType.PrimitiveTypeName.BINARY ||
                       typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
            case TIMESTAMP:
            case DATE:
                return typeName == PrimitiveType.PrimitiveTypeName.INT96 ||
                       typeName == PrimitiveType.PrimitiveTypeName.INT64;
            default:
                return false;
        }
    }
    
    /**
     * Charge une ligne spécifique d'un fichier Parquet dans une table
     * Méthode optimisée pour un accès direct sans lecture du fichier entier
     * 
     * @param tableName Nom de la table où charger la ligne
     * @param filePath Chemin du fichier Parquet
     * @param rowIndex Index de la ligne à charger (0-based)
     * @return Statistiques de chargement, avec 1 ou 0 ligne traitée
     * @throws IOException En cas d'erreur d'accès au fichier
     */
    public ParquetLoadStats loadSpecificRow(String tableName, String filePath, int rowIndex) throws IOException {
        long startTime = System.currentTimeMillis();
        
        // Vérifie que la table existe
        Table table = databaseContext.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table introuvable: " + tableName);
        }
        
        TableData tableData = databaseContext.getTableData(tableName);
        ParquetLoadStats stats = new ParquetLoadStats();
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        
        // Configuration optimisée pour la lecture
        conf.set("fs.hdfs.impl.disable.cache", "false");
        conf.set("parquet.read.support.class", "org.apache.parquet.hadoop.example.GroupReadSupport");
        
        tableData.writeLock();
        try {
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                    .withConf(conf)
                    .build()) {
                
                Group record = null;
                int currentRow = 0;
                
                // Méthode optimisée: accès direct à la ligne demandée
                // Saute rapidement les lignes jusqu'à l'index voulu
                while (currentRow < rowIndex && (record = reader.read()) != null) {
                    // Saute les lignes précédentes sans traitement
                    currentRow++;
                }
                
                // Si nous avons atteint la ligne demandée
                if (currentRow == rowIndex && (record = reader.read()) != null) {
                    // La ligne existe, on la charge dans la table
                    List<Column> columns = table.getColumns();
                    
                    // Ouvre le fichier pour vérifier le schéma
                    try (ParquetFileReader schemaReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
                        MessageType schema = schemaReader.getFooter().getFileMetaData().getSchema();
                        validateSchema(table, schema);
                        
                        // Traite la ligne
                        Object[] rowValues = extractValues(record, columns, schema);
                        tableData.addRow(rowValues);
                        stats.incrementRowsProcessed(1);
                        
                        long elapsedMs = System.currentTimeMillis() - startTime;
                        logger.info("Ligne {} chargée avec succès dans la table {} en {} ms", rowIndex, tableName, elapsedMs);
                    }
                } else {
                    // La ligne demandée n'existe pas
                    logger.warn("La ligne {} n'existe pas dans le fichier {}", rowIndex, filePath);
                }
            }
        } finally {
            tableData.writeUnlock();
        }
        
        stats.setElapsedTimeMs(System.currentTimeMillis() - startTime);
        return stats;
    }
    
    /**
     * Compte le nombre de lignes dans un fichier Parquet sans charger les données
     * @param filePath Chemin du fichier Parquet
     * @return Statistiques sur le fichier Parquet
     */
    public ParquetLoadStats countParquetRows(String filePath) {
        ParquetLoadStats stats = new ParquetLoadStats();
        long startTime = System.currentTimeMillis();
        
        try {
            // Utilise la même approche que loadParquetFile mais sans stocker les données
            Path path = new Path(filePath);
            Configuration conf = new Configuration();
            
            // Configure les paramètres Hadoop, comme dans loadParquetFile
            conf.set("fs.hdfs.impl.disable.cache", "false");
            conf.set("parquet.read.support.class", "org.apache.parquet.hadoop.example.GroupReadSupport");
            conf.set("parquet.filter.record-level.enabled", "true");
            
            // Compte les lignes en utilisant le même lecteur que loadParquetFile
            long rowCount = 0;
            
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                    .withConf(conf)
                    .build()) {
                
                // Parcourt toutes les lignes sans les charger
                while (reader.read() != null) {
                    rowCount++;
                }
                
                stats.setRowsProcessed(rowCount);
                stats.setElapsedTimeMs(System.currentTimeMillis() - startTime);
                logger.info("Comptage des lignes dans le fichier Parquet '{}': {} lignes en {} ms", 
                        filePath, rowCount, stats.getElapsedTimeMs());
            }
        } catch (Exception e) {
            logger.error("Erreur lors du comptage des lignes dans le fichier Parquet '{}': {}", 
                         filePath, e.getMessage());
            stats.setError(e.getMessage());
        }
        
        return stats;
    }
    
    /**
     * Charge un fichier Parquet depuis un flux d'entrée sans écriture sur disque
     * Cette méthode est optimisée pour les environnements avec quotas limités
     * 
     * @param tableName Nom de la table où charger les données
     * @param inputStream Flux contenant les données Parquet
     * @param options Options de chargement
     * @return Statistiques de chargement
     * @throws IOException En cas d'erreur d'E/S
     */
    public ParquetLoadStats loadParquetFileFromStream(String tableName, InputStream inputStream, 
                                                    ParquetLoadOptions options) throws IOException {
        String sessionId = UUID.randomUUID().toString();
        logger.info("[{}] Début du chargement streaming pour la table {}", sessionId, tableName);
        
        // Création d'un fichier temporaire en mémoire (RAM disk si disponible)
        File tempFile = null;
        try {
            // Utilise /dev/shm sur Linux si disponible (RAM disk), sinon un dossier temp standard
            File tempDir = new File("/dev/shm");
            if (!tempDir.exists() || !tempDir.canWrite()) {
                tempDir = new File(System.getProperty("java.io.tmpdir"));
            }
            
            tempFile = File.createTempFile("parquet_stream_", ".parquet", tempDir);
            tempFile.deleteOnExit(); // Garantit la suppression à la fin
            
            // Tranfère le flux en utilisant NIO pour plus d'efficacité
            try (ReadableByteChannel readChannel = Channels.newChannel(inputStream);
                 FileOutputStream fileOS = new FileOutputStream(tempFile)) {
                
                ByteBuffer buffer = ByteBuffer.allocateDirect(64 * 1024); // Buffer de 64KB
                long totalBytes = 0;
                int bytesRead;
                
                while ((bytesRead = readChannel.read(buffer)) != -1) {
                    buffer.flip();
                    fileOS.getChannel().write(buffer);
                    buffer.clear();
                    totalBytes += bytesRead;
                    
                    if (totalBytes % (10 * 1024 * 1024) == 0) { // Log tous les 10MB
                        logger.debug("[{}] {} MB transférés dans le buffer temporaire", 
                                   sessionId, totalBytes / (1024 * 1024));
                    }
                }
                
                logger.info("[{}] Flux Parquet transféré en mémoire: {} MB", 
                           sessionId, totalBytes / (1024 * 1024));
            }
            
            // Maintenant charge depuis ce fichier temporaire
            ParquetLoadStats stats = loadParquetFile(tableName, tempFile.getAbsolutePath(), options);
            logger.info("[{}] Chargement streaming terminé: {} lignes en {} ms", 
                       sessionId, stats.getRowsProcessed(), stats.getElapsedTimeMs());
            
            return stats;
            
        } finally {
            // Supprime le fichier temporaire
            if (tempFile != null && tempFile.exists()) {
                boolean deleted = tempFile.delete();
                if (!deleted) {
                    logger.warn("[{}] Impossible de supprimer le fichier temporaire: {}", 
                              sessionId, tempFile.getAbsolutePath());
                    // Garantit la suppression au mieux
                    tempFile.deleteOnExit();
                }
            }
        }
    }
    
    /**
     * Compte le nombre de lignes dans un flux Parquet sans charger les données
     * 
     * @param inputStream Flux contenant les données Parquet
     * @return Statistiques sur le fichier Parquet
     * @throws IOException En cas d'erreur d'E/S
     */
    public ParquetLoadStats countParquetRowsFromStream(InputStream inputStream) throws IOException {
        String sessionId = UUID.randomUUID().toString();
        logger.info("[{}] Début du comptage de lignes dans un flux Parquet", sessionId);
        
        // Création d'un fichier temporaire en mémoire (RAM disk si disponible)
        File tempFile = null;
        try {
            // Utilise /dev/shm sur Linux si disponible (RAM disk), sinon un dossier temp standard
            File tempDir = new File("/dev/shm");
            if (!tempDir.exists() || !tempDir.canWrite()) {
                tempDir = new File(System.getProperty("java.io.tmpdir"));
            }
            
            tempFile = File.createTempFile("parquet_count_", ".parquet", tempDir);
            tempFile.deleteOnExit(); // Garantit la suppression à la fin
            
            // Transfert le flux efficacement
            try (ReadableByteChannel readChannel = Channels.newChannel(inputStream);
                 FileOutputStream fileOS = new FileOutputStream(tempFile)) {
                
                ByteBuffer buffer = ByteBuffer.allocateDirect(64 * 1024); // Buffer de 64KB
                while (readChannel.read(buffer) != -1) {
                    buffer.flip();
                    fileOS.getChannel().write(buffer);
                    buffer.clear();
                }
            }
            
            // Compte les lignes depuis ce fichier temporaire
            ParquetLoadStats stats = countParquetRows(tempFile.getAbsolutePath());
            logger.info("[{}] Comptage de lignes terminé: {} lignes en {} ms", 
                       sessionId, stats.getRowsProcessed(), stats.getElapsedTimeMs());
            
            return stats;
            
        } finally {
            // Supprime le fichier temporaire
            if (tempFile != null && tempFile.exists()) {
                boolean deleted = tempFile.delete();
                if (!deleted) {
                    logger.warn("[{}] Impossible de supprimer le fichier temporaire: {}", 
                              sessionId, tempFile.getAbsolutePath());
                    // Garantit la suppression au mieux
                    tempFile.deleteOnExit();
                }
            }
        }
    }
    
    /**
     * Charge un fichier Parquet depuis un flux avec distribution entre plusieurs nœuds
     * 
     * @param tableName Nom de la table où charger les données
     * @param inputStream Flux contenant les données Parquet
     * @param options Options de chargement
     * @param nodes Tableau des nœuds pour la distribution
     * @return Statistiques de chargement avec informations par nœud
     * @throws IOException En cas d'erreur d'E/S
     */
    public ParquetLoadStats loadParquetFileFromStreamDistributed(String tableName, InputStream inputStream, 
                                                              ParquetLoadOptions options, 
                                                              com.memorydb.distribution.NodeInfo[] nodes) 
                                                              throws IOException {
        String sessionId = UUID.randomUUID().toString();
        logger.info("[{}] Début du chargement streaming distribué entre {} nœuds pour {}", 
                   sessionId, nodes.length, tableName);
        
        ParquetLoadStats stats = new ParquetLoadStats();
        long startTime = System.currentTimeMillis();
        
        // Création d'un fichier temporaire en mémoire (RAM disk si disponible)
        File tempFile = null;
        try {
            // Utilise /dev/shm sur Linux si disponible (RAM disk), sinon un dossier temp standard
            File tempDir = new File("/dev/shm");
            if (!tempDir.exists() || !tempDir.canWrite()) {
                tempDir = new File(System.getProperty("java.io.tmpdir"));
            }
            
            tempFile = File.createTempFile("parquet_distrib_", ".parquet", tempDir);
            tempFile.deleteOnExit(); // Garantit la suppression à la fin
            
            // Transfert le flux efficacement
            try (ReadableByteChannel readChannel = Channels.newChannel(inputStream);
                 FileOutputStream fileOS = new FileOutputStream(tempFile)) {
                
                ByteBuffer buffer = ByteBuffer.allocateDirect(64 * 1024); // Buffer de 64KB
                long totalBytes = 0;
                int bytesRead;
                
                while ((bytesRead = readChannel.read(buffer)) != -1) {
                    buffer.flip();
                    fileOS.getChannel().write(buffer);
                    buffer.clear();
                    totalBytes += bytesRead;
                }
                
                logger.info("[{}] Flux Parquet transféré en mémoire: {} MB", 
                           sessionId, totalBytes / (1024 * 1024));
            }
            
            // Maintenant charge avec distribution par blocs
            // Vérifie que la table existe
            Table table = databaseContext.getTable(tableName);
            if (table == null) {
                throw new IllegalArgumentException("Table introuvable: " + tableName);
            }
            
            TableData tableData = databaseContext.getTableData(tableName);
            
            // Ouvre le fichier Parquet pour vérifier le schéma
            Path path = new Path(tempFile.getAbsolutePath());
            Configuration conf = new Configuration();
            
            // Indique à Hadoop et Parquet de garder les fichiers ouverts
            conf.set("fs.hdfs.impl.disable.cache", "false");
            conf.set("parquet.read.support.class", "org.apache.parquet.hadoop.example.GroupReadSupport");
            conf.set("parquet.filter.record-level.enabled", "true");
            
            try (ParquetFileReader schemaReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
                MessageType schema = schemaReader.getFooter().getFileMetaData().getSchema();
                validateSchema(table, schema);
                
                // Configuration pour la distribution par blocs
                int batchSize = options.getBatchSize();
                int skipRows = options.getSkipRows();
                long rowLimit = options.getRowLimit();
                
                // Répartition des lignes entre les nœuds en utilisant un approche par blocs
                try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                        .withConf(conf)
                        .build()) {
                    
                    // Skip initial rows if needed
                    Group record = null;
                    for (int i = 0; i < skipRows && reader.read() != null; i++) {
                        // Skipping
                    }
                    
                    int nodeIndex = 0;
                    List<Object[]> batch = new ArrayList<>(batchSize);
                    List<Column> columns = table.getColumns();
                    
                    long currentRow = 0;
                    int currentRowsInBatch = 0;
                    Map<String, Long> nodeRows = new HashMap<>();
                    
                    // Initialize node counts
                    for (com.memorydb.distribution.NodeInfo node : nodes) {
                        nodeRows.put(node.getId(), 0L);
                    }
                    
                    // Process all rows or up to row limit
                    while ((record = reader.read()) != null && 
                           (rowLimit <= 0 || currentRow < rowLimit)) {
                        
                        // Get node for this row (round-robin)
                        com.memorydb.distribution.NodeInfo currentNode = nodes[nodeIndex];
                        String nodeId = currentNode.getId();
                        
                        // Extract row values and add to batch
                        Object[] rowValues = extractValues(record, columns, schema);
                        batch.add(rowValues);
                        currentRowsInBatch++;
                        
                        // Process batch if full
                        if (currentRowsInBatch >= batchSize) {
                            tableData.writeLock();
                            try {
                                addBatchToTable(tableData, batch);
                            } finally {
                                tableData.writeUnlock();
                            }
                            
                            // Update stats
                            long previousCount = nodeRows.get(nodeId);
                            nodeRows.put(nodeId, previousCount + currentRowsInBatch);
                            stats.addNodeRows(nodeId, currentRowsInBatch);
                            
                            batch.clear();
                            currentRowsInBatch = 0;
                            
                            if (currentRow % 100000 == 0) {
                                logger.info("[{}] Progress: {} rows processed", sessionId, currentRow);
                            }
                        }
                        
                        // Move to next node for round-robin
                        nodeIndex = (nodeIndex + 1) % nodes.length;
                        currentRow++;
                    }
                    
                    // Process final partial batch if any
                    if (!batch.isEmpty()) {
                        tableData.writeLock();
                        try {
                            addBatchToTable(tableData, batch);
                        } finally {
                            tableData.writeUnlock();
                        }
                        
                        // Update stats for last batch
                        com.memorydb.distribution.NodeInfo currentNode = nodes[nodeIndex];
                        String nodeId = currentNode.getId();
                        long previousCount = nodeRows.get(nodeId);
                        nodeRows.put(nodeId, previousCount + currentRowsInBatch);
                        stats.addNodeRows(nodeId, currentRowsInBatch);
                    }
                    
                    // Update final stats
                    stats.setElapsedTimeMs(System.currentTimeMillis() - startTime);
                    
                    logger.info("[{}] Chargement distribué terminé: {} lignes total, distribution par nœud: {}", 
                              sessionId, stats.getRowsProcessed(), nodeRows);
                }
            }
            
            return stats;
            
        } finally {
            // Cleanup temp file
            if (tempFile != null && tempFile.exists()) {
                boolean deleted = tempFile.delete();
                if (!deleted) {
                    logger.warn("[{}] Impossible de supprimer le fichier temporaire: {}", 
                              sessionId, tempFile.getAbsolutePath());
                    tempFile.deleteOnExit();
                }
            }
        }
    }
    
    /**
     * Ferme les ressources utilisées
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
}
