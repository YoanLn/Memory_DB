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
import java.nio.file.Files;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import com.memorydb.distribution.ClusterManager;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implémentation optimisée pour le chargement de fichiers Parquet 
 * avec support des grands volumes et traitement par batch
 */
@ApplicationScoped
public class VectorizedParquetLoader {
    private static final Logger logger = LoggerFactory.getLogger(VectorizedParquetLoader.class);
    
    @Inject
    private DatabaseContext databaseContext;
    
    @Inject
    private ClusterManager clusterManager;

    private ExecutorService executorService;

    private HttpClient httpClient;
    private final ObjectMapper objectMapper;

    // List to track pending async operations
    private List<CompletableFuture<Void>> pendingOperations = new ArrayList<>();
    
    // Reusable objects cache to reduce GC pressure
    private ThreadLocal<ValueCache> valueCache = ThreadLocal.withInitial(ValueCache::new);
    
    /**
     * Cache of primitive and reusable values to reduce object creation during row extraction
     */
    private static class ValueCache {
        // Reusable arrays to avoid allocations
        Object[] values;
        String[] stringValues;
        int[] intValues;
        long[] longValues;
        float[] floatValues;
        double[] doubleValues;
        boolean[] boolValues;
        
        // Size tracking
        int lastSize = 0;
        
        // Create or resize arrays as needed
        void ensureCapacity(int size) {
            if (values == null || size > lastSize) {
                values = new Object[size];
                stringValues = new String[size];
                intValues = new int[size];
                longValues = new long[size];
                floatValues = new float[size];
                doubleValues = new double[size];
                boolValues = new boolean[size];
                lastSize = size;
            }
        }
    }

    // Création d'un client HTTP pour la distribution inter-nœuds
    public VectorizedParquetLoader() {
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(java.time.Duration.ofSeconds(30))
            .build();
        this.objectMapper = new ObjectMapper();
    }
    
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
     * Ajoute un batch de données à la table de manière optimisée
     * Cette version utilise directement les tableaux primitifs pour minimiser
     * les opérations de boxing/unboxing et réduire la pression sur le GC
     */
    private void addBatchToTable(TableData tableData, List<Object[]> batchData) {
        Table table = tableData.getTable();
        List<Column> columns = table.getColumns();
        int columnCount = columns.size();
        int batchSize = batchData.size();
        
        if (batchSize == 0) {
            return; // Rien à faire
        }
        
        // Récupère le cache thread-local pour les valeurs primitives
        ValueCache cache = valueCache.get();
        cache.ensureCapacity(columnCount);
        
        // Pré-trie les données par type pour réduire les opérations de boxing/unboxing
        int[] columnTypes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ColumnStore store = tableData.getColumnStore(i);
            columnTypes[i] = store.getType().ordinal();
        }
        
        tableData.writeLock();
        try {
            // Traite le batch ligne par ligne
            for (Object[] rowValues : batchData) {
                // Vérification pour éviter les problèmes d'index
                if (rowValues.length != columnCount) {
                    throw new IllegalArgumentException("Nombre de valeurs incorrect, attendu: " + 
                        columnCount + ", obtenu: " + rowValues.length);
                }
                
                // Extrait les valeurs dans les tableaux primitifs appropriés
                for (int i = 0; i < columnCount; i++) {
                    Object value = rowValues[i];
                    ColumnStore columnStore = tableData.getColumnStore(i);
                    
                    if (value == null) {
                        columnStore.addNull();
                        continue;
                    }
                    
                    // Utilise un switch sans boxing/unboxing quand possible
                    switch (columnStore.getType()) {
                        case INTEGER:
                            if (value instanceof Integer) {
                                columnStore.addInt((Integer) value);
                            } else if (value instanceof Number) {
                                // Conversion sans création d'objet intermédiaire
                                columnStore.addInt(((Number) value).intValue());
                            } else {
                                columnStore.addInt(Integer.parseInt(value.toString()));
                            }
                            break;
                        case LONG:
                            if (value instanceof Long) {
                                columnStore.addLong((Long) value);
                            } else if (value instanceof Number) {
                                // Conversion sans création d'objet intermédiaire
                                columnStore.addLong(((Number) value).longValue());
                            } else {
                                columnStore.addLong(Long.parseLong(value.toString()));
                            }
                            break;
                        case FLOAT:
                            if (value instanceof Float) {
                                columnStore.addFloat((Float) value);
                            } else if (value instanceof Number) {
                                // Conversion sans création d'objet intermédiaire
                                columnStore.addFloat(((Number) value).floatValue());
                            } else {
                                columnStore.addFloat(Float.parseFloat(value.toString()));
                            }
                            break;
                        case DOUBLE:
                            if (value instanceof Double) {
                                columnStore.addDouble((Double) value);
                            } else if (value instanceof Number) {
                                // Conversion sans création d'objet intermédiaire
                                columnStore.addDouble(((Number) value).doubleValue());
                            } else {
                                columnStore.addDouble(Double.parseDouble(value.toString()));
                            }
                            break;
                        case BOOLEAN:
                            if (value instanceof Boolean) {
                                columnStore.addBoolean((Boolean) value);
                            } else {
                                columnStore.addBoolean(Boolean.parseBoolean(value.toString()));
                            }
                            break;
                        case STRING:
                            // Utilise directement .intern() pour réduire les duplications de string
                            if (value instanceof String) {
                                columnStore.addString(((String) value).intern());
                            } else {
                                columnStore.addString(value.toString().intern());
                            }
                            break;
                        case DATE:
                        case TIMESTAMP:
                            if (value instanceof Long) {
                                columnStore.addDate((Long) value);
                            } else if (value instanceof Number) {
                                // Conversion sans création d'objet intermédiaire
                                columnStore.addDate(((Number) value).longValue());
                            } else {
                                columnStore.addDate(Long.parseLong(value.toString()));
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("Type non supporté: " + columnStore.getType());
                    }
                }
                
                // Incrémente le compteur de lignes
                tableData.incrementRowCount();
            }
            
            logger.debug("Batch de {} lignes ajouté à la table", batchSize);
        } finally {
            tableData.writeUnlock();
        }
    }
    
    /**
     * Extrait les valeurs d'un groupe Parquet de manière optimisée
     * en utilisant des types primitifs et des objets réutilisables
     * pour réduire la pression sur le garbage collector
     */
    private Object[] extractValues(Group group, List<Column> columns, MessageType schema) {
        // Get thread-local cache for reusable objects
        ValueCache cache = valueCache.get();
        int size = columns.size();
        cache.ensureCapacity(size);
        
        // Alias local variables for better performance
        Object[] values = cache.values;
        int[] intValues = cache.intValues;
        long[] longValues = cache.longValues;
        float[] floatValues = cache.floatValues;
        double[] doubleValues = cache.doubleValues;
        boolean[] boolValues = cache.boolValues;
        String[] stringValues = cache.stringValues;
        
        for (int i = 0; i < size; i++) {
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
            
            // Extraction de la valeur selon le type et stockage dans les tableaux primitifs
            switch (column.getType()) {
                case INTEGER:
                    intValues[i] = group.getInteger(fieldName, 0);
                    values[i] = intValues[i]; // Boxing only when needed
                    break;
                case LONG:
                    longValues[i] = group.getLong(fieldName, 0);
                    values[i] = longValues[i]; // Boxing only when needed
                    break;
                case FLOAT:
                    floatValues[i] = group.getFloat(fieldName, 0);
                    values[i] = floatValues[i]; // Boxing only when needed
                    break;
                case DOUBLE:
                    doubleValues[i] = group.getDouble(fieldName, 0);
                    values[i] = doubleValues[i]; // Boxing only when needed
                    break;
                case BOOLEAN:
                    boolValues[i] = group.getBoolean(fieldName, 0);
                    values[i] = boolValues[i]; // Boxing only when needed
                    break;
                case STRING:
                    Binary binary = group.getBinary(fieldName, 0);
                    // Use string interning to reduce memory usage for repeated strings
                    stringValues[i] = binary.toStringUsingUTF8().intern();
                    values[i] = stringValues[i];
                    break;
                case DATE:
                case TIMESTAMP:
                    if (parquetField.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
                        Binary int96Value = group.getInt96(fieldName, 0);
                        longValues[i] = convertInt96ToTimestamp(int96Value);
                        values[i] = longValues[i];
                    } else {
                        longValues[i] = group.getLong(fieldName, 0);
                        values[i] = longValues[i];
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
        
        // More efficient streaming with larger buffers
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8 * 1024 * 1024); // Pre-allocate 8MB to reduce reallocations
        long totalBytesTransferred = 0;
        try (InputStream bufferedInputStream = new BufferedInputStream(inputStream, 256 * 1024)) { // 256KB buffer
            byte[] byteParquetLoaderBuffer = new byte[512 * 1024]; // 512KB read buffer for better throughput
            int bytesRead;
            while ((bytesRead = bufferedInputStream.read(byteParquetLoaderBuffer)) != -1) {
                baos.write(byteParquetLoaderBuffer, 0, bytesRead);
                totalBytesTransferred += bytesRead;
            }
        }
        logger.info("[{}] Flux Parquet lu en mémoire: {} MB",
                   sessionId, totalBytesTransferred / (1024 * 1024));

        byte[] parquetBytes = baos.toByteArray();
        ByteBuffer parquetByteBuffer = ByteBuffer.wrap(parquetBytes);
        baos.close(); // Release memory from ByteArrayOutputStream

        // Vérifie que la table existe
        Table table = databaseContext.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table introuvable: " + tableName);
        }
        
        TableData tableData = databaseContext.getTableData(tableName);
        
        Configuration conf = new Configuration();
        // Indique à Hadoop et Parquet de garder les fichiers ouverts
        conf.set("fs.hdfs.impl.disable.cache", "false");
        conf.set("parquet.read.support.class", "org.apache.parquet.hadoop.example.GroupReadSupport");
        conf.set("parquet.filter.record-level.enabled", "true");

        java.nio.file.Path tempParquetFile = null;
        try {
            // Write to a temporary file, but with optimizations:
            // 1. Use a more descriptive name for better debugging
            // 2. Use buffered I/O for faster writes
            // 3. Set a specific Files.DELETE_ON_CLOSE attribute for automatic cleanup
            tempParquetFile = Files.createTempFile("memorydb-parquet-" + sessionId + "-", ".parquet");
            
            // Make the file delete on JVM exit as a safety measure
            tempParquetFile.toFile().deleteOnExit();
            
            // Write directly from ByteBuffer to file with minimal copying
            try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(tempParquetFile, 
                    java.nio.file.StandardOpenOption.WRITE)) {
                channel.write(parquetByteBuffer);
            }
            
            // Use standard Hadoop Path API which is fully compatible with Parquet 1.13.1
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempParquetFile.toUri());
            
            // Use modern ParquetFileReader API with HadoopInputFile to avoid deprecation warnings
            try (ParquetFileReader schemaReader = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, conf))) {
            MessageType schema = schemaReader.getFooter().getFileMetaData().getSchema();
            validateSchema(table, schema);
            
            // Configuration pour la distribution par blocs
            int batchSize = options.getBatchSize();
            int skipRows = options.getSkipRows();
            long rowLimit = options.getRowLimit();
            
            // Use the standard builder pattern that's compatible with Parquet 1.13.1
            try (ParquetReader<Group> reader = ParquetReader.<Group>builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(tempParquetFile.toUri()))
                    .withConf(conf)
                    .build()) {
                
                // Skip initial rows if needed
                Group record = null;
                for (int i = 0; i < skipRows && reader.read() != null; i++) {
                    // Skipping
                }
                
                // Initialisation for node distribution
                int nodeIndex = 0;
                List<Object[]> batch = new ArrayList<>(batchSize);
                List<Column> columns = table.getColumns();
                
                // Make sure we properly distribute data across all nodes
                int nodeCount = nodes.length;
                logger.info("[{}] Configuré pour distribuer les données entre {} nœuds en round-robin", sessionId, nodeCount);
                
                long currentRow = 0;
                int currentRowsInBatch = 0;
                Map<String, Long> nodeRows = new HashMap<>();
                
                // Initialize the nodeRows map for proper distribution tracking
                for (com.memorydb.distribution.NodeInfo node : nodes) {
                    nodeRows.put(node.getId(), 0L);
                    logger.info("[{}] Initialisation du compteur pour le nœud {}", sessionId, node.getId());
                }
                                
                // Process all rows or up to row limit
                while ((record = reader.read()) != null && 
                       (rowLimit <= 0 || currentRow < rowLimit)) {
                        
                        // Get node for this row (round-robin)
                        int actualNodeIndex = (int)(currentRow % nodes.length);
                        com.memorydb.distribution.NodeInfo currentNode = nodes[actualNodeIndex];
                        String nodeId = currentNode.getId();
                        
                        if (currentRow % 100000 == 0) {
                            logger.info("[{}] Ligne {} attribuée au nœud {} (index {})", 
                                sessionId, currentRow, nodeId, actualNodeIndex);
                        }
                        
                        // Extract row values and add to batch
                        Object[] rowValues = extractValues(record, columns, schema);
                        batch.add(rowValues);
                        currentRowsInBatch++;
                        
                        // Process batch if full
                        if (currentRowsInBatch >= batchSize) {
                            // Si c'est le nœud local, ajouter directement à la table locale
                            final String localNodeId = clusterManager.getLocalNode().getId();
                            final String finalNodeId = nodeId;
                            final com.memorydb.distribution.NodeInfo finalNode = currentNode;
                            final List<Object[]> batchToProcess = new ArrayList<>(batch); // Create a copy to avoid concurrent modification
                            final int finalBatchSize = currentRowsInBatch;
                            
                            if (nodeId.equals(localNodeId)) {
                                // Process local node synchronously to avoid too many threads 
                                // and potential lock contention on tableData
                                tableData.writeLock();
                                try {
                                    addBatchToTable(tableData, batchToProcess);
                                } finally {
                                    tableData.writeUnlock();
                                }
                                
                                // Update stats immediately for local node
                                long previousCount = nodeRows.get(finalNodeId);
                                nodeRows.put(finalNodeId, previousCount + finalBatchSize);
                                stats.addNodeRows(finalNodeId, finalBatchSize);
                            } else {
                                // For remote nodes, use CompletableFuture to process batches in parallel
                                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                                    try {
                                        // Send batch to remote node asynchronously
                                        sendBatchToRemoteNode(finalNode, tableName, batchToProcess);
                                        
                                        // Update stats after successful send
                                        synchronized (nodeRows) {
                                            long previousCount = nodeRows.get(finalNodeId);
                                            nodeRows.put(finalNodeId, previousCount + finalBatchSize);
                                            stats.addNodeRows(finalNodeId, finalBatchSize);
                                        }
                                    } catch (Exception e) {
                                        logger.error("[{}] Erreur lors de l'envoi asynchrone des données au nœud {}: {}", 
                                                sessionId, finalNodeId, e.getMessage(), e);
                                        // We don't throw here since we're in an async context
                                        // Instead we log the error and continue
                                    }
                                });
                                // Track this operation
                                pendingOperations.add(future);
                            }
                            
                            batch.clear();
                            currentRowsInBatch = 0;
                            
                            if (currentRow % 100000 == 0) {
                                logger.info("[{}] Progress: {} rows processed. Distribution actuelle: {}", 
                                           sessionId, currentRow, nodeRows);
                            }
                        }
                        
                        // We now use the modulo-based distribution in the node selection above
                        // so we only need to increment the row counter here
                        currentRow++;
                    }
                    
                    // Process final partial batch if any
                    if (!batch.isEmpty()) {
                        // Determine the correct node for this final batch
                        // The nodeIndex was already incremented for the *next* batch, so we use the previous one.
                        int finalBatchNodeIndex = (nodeIndex == 0) ? (nodes.length - 1) : (nodeIndex - 1);
                        if (nodes.length == 1) finalBatchNodeIndex = 0; // Handle single node case

                        com.memorydb.distribution.NodeInfo finalBatchNode = nodes[finalBatchNodeIndex];
                        String finalBatchNodeId = finalBatchNode.getId();
                        final List<Object[]> finalBatch = new ArrayList<>(batch); // Make a copy to avoid concurrent modification
                        final int finalBatchSize = batch.size();

                        String localNodeId = clusterManager.getLocalNode().getId();
                        if (finalBatchNodeId.equals(localNodeId)) {
                            tableData.writeLock();
                            try {
                                addBatchToTable(tableData, finalBatch);
                            } finally {
                                tableData.writeUnlock();
                            }
                            
                            // Update stats for local batch immediately
                            long previousCount = nodeRows.get(finalBatchNodeId);
                            nodeRows.put(finalBatchNodeId, previousCount + finalBatchSize);
                            stats.addNodeRows(finalBatchNodeId, finalBatchSize);
                        } else {
                            // For remote nodes, process asynchronously
                            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                                try {
                                    sendBatchToRemoteNode(finalBatchNode, tableName, finalBatch);
                                    
                                    // Update stats after successful send
                                    synchronized (nodeRows) {
                                        long prevCount = nodeRows.get(finalBatchNodeId);
                                        nodeRows.put(finalBatchNodeId, prevCount + finalBatchSize);
                                        stats.addNodeRows(finalBatchNodeId, finalBatchSize);
                                    }
                                } catch (Exception e) {
                                    logger.error("[{}] Erreur lors de l'envoi asynchrone du batch final au nœud {}: {}", 
                                             sessionId, finalBatchNodeId, e.getMessage(), e);
                                }
                            });
                            pendingOperations.add(future);
                        }
                    }
                    
                    // Update final stats
                    stats.setElapsedTimeMs(System.currentTimeMillis() - startTime);
                    
                    logger.info("[{}] Chargement distribué terminé: {} lignes total, distribution par nœud: {}", 
                              sessionId, stats.getRowsProcessed(), nodeRows);
                }
            }
            
            // Wait for all pending async operations to complete before returning
            if (!pendingOperations.isEmpty()) {
                logger.info("[{}] Waiting for {} pending batch operations to complete", sessionId, pendingOperations.size());
                try {
                    CompletableFuture.allOf(pendingOperations.toArray(new CompletableFuture[0])).join();
                } catch (Exception e) {
                    logger.error("[{}] Error waiting for async batch operations: {}", sessionId, e.getMessage(), e);
                }
            }
            
            return stats;
        } catch (Exception e) {
            logger.error("[{}] Error during Parquet processing: {}", sessionId, e.getMessage(), e);
            throw e;
        } finally {
            // Clean up the temporary file
            if (tempParquetFile != null) {
                try {
                    Files.deleteIfExists(tempParquetFile);
                } catch (IOException e) {
                    logger.warn("[{}] Could not delete temporary file: {}", sessionId, tempParquetFile, e);
                }
            }
        }
    }

    /**
     * Ferme les ressources utilisées
     */
    /**
     * Envoie un batch de données à un nœud distant avec une sérialisation optimisée
     * Cette version utilise un format basé sur les colonnes plutôt que sur les lignes
     * pour réduire la taille de la sérialisation et améliorer les performances
     * 
     * @param node Nœud distant où envoyer les données
     * @param tableName Nom de la table à mettre à jour
     * @param batch Liste d'objets représentant les lignes à ajouter
     * @throws IOException En cas d'erreur d'E/S ou de communication
     */
    private void sendBatchToRemoteNode(com.memorydb.distribution.NodeInfo node, String tableName, 
                                      List<Object[]> batch) throws IOException {
        try {
            if (batch.isEmpty()) {
                return; // Rien à envoyer
            }
            
            int rowCount = batch.size();
            int columnCount = batch.get(0).length;
            
            // Crée une structure basée sur les colonnes pour réduire l'overhead JSON
            // Cela permet de sérialiser chaque colonne comme un tableau homogène
            // au lieu d'avoir des objets hétérogènes pour chaque ligne
            List<Map<String, Object>> columns = new ArrayList<>(columnCount);
            
            for (int colIndex = 0; colIndex < columnCount; colIndex++) {
                Map<String, Object> column = new HashMap<>();
                
                // Détermine le type de la colonne en inspectant les valeurs non nulles
                String columnType = "unknown";
                for (Object[] row : batch) {
                    if (row[colIndex] != null) {
                        if (row[colIndex] instanceof Integer) columnType = "int";
                        else if (row[colIndex] instanceof Long) columnType = "long";
                        else if (row[colIndex] instanceof Float) columnType = "float";
                        else if (row[colIndex] instanceof Double) columnType = "double";
                        else if (row[colIndex] instanceof Boolean) columnType = "boolean";
                        else if (row[colIndex] instanceof String) columnType = "string";
                        break;
                    }
                }
                
                column.put("type", columnType);
                
                // Crée les tableaux de valeurs homogènes pour chaque type
                switch (columnType) {
                    case "int":
                        int[] intValues = new int[rowCount];
                        boolean[] intNulls = new boolean[rowCount];
                        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                            Object value = batch.get(rowIndex)[colIndex];
                            if (value == null) {
                                intNulls[rowIndex] = true;
                            } else {
                                intValues[rowIndex] = value instanceof Number ? 
                                    ((Number) value).intValue() : Integer.parseInt(value.toString());
                            }
                        }
                        column.put("values", intValues);
                        column.put("nulls", intNulls);
                        break;
                        
                    case "long":
                        long[] longValues = new long[rowCount];
                        boolean[] longNulls = new boolean[rowCount];
                        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                            Object value = batch.get(rowIndex)[colIndex];
                            if (value == null) {
                                longNulls[rowIndex] = true;
                            } else {
                                longValues[rowIndex] = value instanceof Number ? 
                                    ((Number) value).longValue() : Long.parseLong(value.toString());
                            }
                        }
                        column.put("values", longValues);
                        column.put("nulls", longNulls);
                        break;
                        
                    case "float":
                        float[] floatValues = new float[rowCount];
                        boolean[] floatNulls = new boolean[rowCount];
                        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                            Object value = batch.get(rowIndex)[colIndex];
                            if (value == null) {
                                floatNulls[rowIndex] = true;
                            } else {
                                floatValues[rowIndex] = value instanceof Number ? 
                                    ((Number) value).floatValue() : Float.parseFloat(value.toString());
                            }
                        }
                        column.put("values", floatValues);
                        column.put("nulls", floatNulls);
                        break;
                        
                    case "double":
                        double[] doubleValues = new double[rowCount];
                        boolean[] doubleNulls = new boolean[rowCount];
                        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                            Object value = batch.get(rowIndex)[colIndex];
                            if (value == null) {
                                doubleNulls[rowIndex] = true;
                            } else {
                                doubleValues[rowIndex] = value instanceof Number ? 
                                    ((Number) value).doubleValue() : Double.parseDouble(value.toString());
                            }
                        }
                        column.put("values", doubleValues);
                        column.put("nulls", doubleNulls);
                        break;
                        
                    case "boolean":
                        boolean[] boolValues = new boolean[rowCount];
                        boolean[] boolNulls = new boolean[rowCount];
                        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                            Object value = batch.get(rowIndex)[colIndex];
                            if (value == null) {
                                boolNulls[rowIndex] = true;
                            } else {
                                boolValues[rowIndex] = value instanceof Boolean ? 
                                    (Boolean) value : Boolean.parseBoolean(value.toString());
                            }
                        }
                        column.put("values", boolValues);
                        column.put("nulls", boolNulls);
                        break;
                        
                    case "string":
                    default:
                        // Pour les strings et types inconnus, utilise un tableau de strings
                        String[] stringValues = new String[rowCount];
                        boolean[] stringNulls = new boolean[rowCount];
                        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                            Object value = batch.get(rowIndex)[colIndex];
                            if (value == null) {
                                stringNulls[rowIndex] = true;
                            } else {
                                stringValues[rowIndex] = value.toString();
                            }
                        }
                        column.put("values", stringValues);
                        column.put("nulls", stringNulls);
                        break;
                }
                
                columns.add(column);
            }
            
            // Construction du payload optimisé
            Map<String, Object> payload = new HashMap<>();
            payload.put("tableName", tableName);
            payload.put("format", "column-based");
            payload.put("rowCount", rowCount);
            payload.put("columnCount", columnCount);
            payload.put("columns", columns);
            
            // Construction de l'URL du nœud distant
            String url = String.format("http://%s:%d/api/tables/%s/add-batch-columnar", 
                    node.getAddress(), node.getPort(), tableName);
            
            // Convertit le payload en JSON
            String jsonPayload = objectMapper.writeValueAsString(payload);
            
            // Préparation de la requête HTTP
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .build();
            
            // Envoi de la requête
            HttpResponse<String> response = httpClient.send(request, 
                    HttpResponse.BodyHandlers.ofString());
            
            // Vérification de la réponse
            if (response.statusCode() != 200) {
                throw new IOException("Erreur lors de l'envoi du batch au nœud " + node.getId() + 
                        ": HTTP " + response.statusCode() + " - " + response.body());
            }
            
            logger.debug("Batch columnar de {} lignes envoyé au nœud {}", rowCount, node.getId());
            
        } catch (URISyntaxException | InterruptedException e) {
            throw new IOException("Erreur lors de la communication avec le nœud distant: " + e.getMessage(), e);
        }
    }
    
    // Méthode asynchrone retirée car non utilisée dans l'implémentation actuelle

    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
}
