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
import java.util.Arrays;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;

/**
 * ULTRA-AGGRESSIVE Parquet loader with extreme performance optimizations
 * - Connection pooling with persistent HTTP clients
 * - Massive parallel processing with custom thread pools
 * - Zero-copy binary operations
 * - Async streaming with pipeline parallelism
 * - 10x larger batch sizes for maximum throughput
 */
@ApplicationScoped
public class VectorizedParquetLoader {
    private static final Logger logger = LoggerFactory.getLogger(VectorizedParquetLoader.class);
    
    @Inject
    private DatabaseContext databaseContext;
    
    @Inject
    private ClusterManager clusterManager;

    // ULTRA-AGGRESSIVE: Massive thread pools for maximum parallelism
    private static final int ULTRA_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 8;
    private static final int NETWORK_THREAD_POOL_SIZE = 64; // Dedicated network threads
    private static final int PROCESSING_THREAD_POOL_SIZE = 32; // Dedicated processing threads
    
    private ExecutorService ultraExecutorService;
    private ExecutorService networkExecutorService;
    private ExecutorService processingExecutorService;

    // ULTRA-AGGRESSIVE: Persistent HTTP client pool with connection reuse
    private static final Map<String, HttpClient> HTTP_CLIENT_POOL = new HashMap<>();
    private static final Object CLIENT_POOL_LOCK = new Object();
    
    private final ObjectMapper objectMapper;

    // ULTRA-AGGRESSIVE: Massive batch sizes for maximum throughput
    private static final int ULTRA_BATCH_SIZE = 2_000_000; // 2M rows per batch
    private static final int MEGA_BATCH_SIZE = 5_000_000; // 5M rows for local processing

    // List to track pending async operations
    private List<CompletableFuture<Void>> pendingOperations = new ArrayList<>();
    
    // Reusable objects cache to reduce GC pressure
    private ThreadLocal<ValueCache> valueCache = ThreadLocal.withInitial(ValueCache::new);
    
    // ULTRA-AGGRESSIVE: Larger buffers for maximum I/O throughput
    private static final int ULTRA_BUFFER_SIZE = 2 * 1024 * 1024; // 2MB buffer
    private ThreadLocal<byte[]> bufferCache = ThreadLocal.withInitial(() -> new byte[ULTRA_BUFFER_SIZE]);
    
    // Ultra-fast Kryo serialization
    private ThreadLocal<Kryo> kryoCache = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.setReferences(false); // Disable references for better performance
        return kryo;
    });
    
    // ULTRA-AGGRESSIVE: Performance monitoring
    private final AtomicLong totalRowsProcessed = new AtomicLong(0);
    private final AtomicLong totalBytesTransferred = new AtomicLong(0);
    private final AtomicLong networkOperations = new AtomicLong(0);
    
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
        
        // Clear references to help GC
        void clear() {
            if (values != null) {
                Arrays.fill(values, null);
                Arrays.fill(stringValues, null);
            }
        }
    }

    // ULTRA-AGGRESSIVE: Initialize massive thread pools and persistent HTTP clients
    public VectorizedParquetLoader() {
        this.objectMapper = new ObjectMapper();
        initializeUltraThreadPools();
        initializePersistentHttpClients();
    }
    
    /**
     * ULTRA-AGGRESSIVE: Initialize massive thread pools for maximum parallelism
     */
    private void initializeUltraThreadPools() {
        // Ultra-massive general purpose thread pool
        ultraExecutorService = new ThreadPoolExecutor(
            ULTRA_THREAD_POOL_SIZE,
            ULTRA_THREAD_POOL_SIZE * 2,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10000),
            r -> {
                Thread t = new Thread(r, "Ultra-Processor-" + System.nanoTime());
                t.setDaemon(false);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        );
        
        // Dedicated network thread pool for HTTP operations
        networkExecutorService = new ThreadPoolExecutor(
            NETWORK_THREAD_POOL_SIZE,
            NETWORK_THREAD_POOL_SIZE,
            30L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(5000),
            r -> {
                Thread t = new Thread(r, "Network-Ultra-" + System.nanoTime());
                t.setDaemon(false);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        );
        
        // Dedicated processing thread pool for data operations
        processingExecutorService = new ThreadPoolExecutor(
            PROCESSING_THREAD_POOL_SIZE,
            PROCESSING_THREAD_POOL_SIZE,
            30L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(5000),
            r -> {
                Thread t = new Thread(r, "Processing-Ultra-" + System.nanoTime());
                t.setDaemon(false);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        );
        
        logger.info("ULTRA-AGGRESSIVE thread pools initialized: {} ultra threads, {} network threads, {} processing threads",
                ULTRA_THREAD_POOL_SIZE, NETWORK_THREAD_POOL_SIZE, PROCESSING_THREAD_POOL_SIZE);
    }
    
    /**
     * ULTRA-AGGRESSIVE: Initialize persistent HTTP clients with connection pooling
     */
    private void initializePersistentHttpClients() {
        synchronized (CLIENT_POOL_LOCK) {
            if (HTTP_CLIENT_POOL.isEmpty()) {
                // Create persistent HTTP clients for each potential node
                for (int i = 1; i <= 10; i++) { // Support up to 10 nodes
                    String nodeKey = "node" + i;
                    HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(java.time.Duration.ofSeconds(5)) // Faster connection timeout
                        .executor(networkExecutorService) // Use dedicated network threads
            .build();
                    HTTP_CLIENT_POOL.put(nodeKey, client);
                }
                logger.info("ULTRA-AGGRESSIVE: Initialized {} persistent HTTP clients", HTTP_CLIENT_POOL.size());
            }
        }
    }
    
    /**
     * ULTRA-AGGRESSIVE: Get or create persistent HTTP client for a node
     */
    private HttpClient getHttpClientForNode(String nodeId) {
        synchronized (CLIENT_POOL_LOCK) {
            return HTTP_CLIENT_POOL.computeIfAbsent(nodeId, id -> {
                HttpClient client = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(java.time.Duration.ofSeconds(5))
                    .executor(networkExecutorService)
                    .build();
                logger.debug("Created new persistent HTTP client for node: {}", nodeId);
                return client;
            });
        }
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
        if (ultraExecutorService == null || ultraExecutorService.isShutdown()) {
            ultraExecutorService = Executors.newFixedThreadPool(Math.min(options.getParallelism(), 8));
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
        
        // Optimized Parquet configuration for memory efficiency
        conf.set("fs.hdfs.impl.disable.cache", "false");
        conf.set("parquet.read.support.class", "org.apache.parquet.hadoop.example.GroupReadSupport");
        conf.set("parquet.filter.record-level.enabled", "true");
        // Memory optimizations
        conf.set("parquet.page.size", "1048576"); // 1MB page size for better memory usage
        conf.set("parquet.dictionary.page.size", "1048576"); // 1MB dictionary page size
        conf.setInt("parquet.read.allocation.size", 8 * 1024 * 1024); // 8MB read buffer
        
        try (ParquetFileReader schemaReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType schema = schemaReader.getFooter().getFileMetaData().getSchema();
            validateSchema(table, schema);
            
            // Configuration pour le streaming par batch - use much larger batches for better performance
            long rowLimit = options.getRowLimit();
            int batchSize = Math.max(options.getBatchSize(), 500000); // Minimum 500k batch size
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
                            
                            // Clear value cache and log less frequently for better performance
                            if (batchCount % 50 == 0) {
                                valueCache.get().clear();
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
                                // Optimized batch processing with reduced method calls
                    int batchRowCount = batchData.size();
                    
                    // Pre-allocate arrays for bulk operations
                    ColumnStore[] columnStores = new ColumnStore[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        columnStores[i] = tableData.getColumnStore(i);
                    }
                    
                    // Process batch with minimal object creation
                    for (int rowIdx = 0; rowIdx < batchRowCount; rowIdx++) {
                        Object[] rowValues = batchData.get(rowIdx);
                        
                        // Quick validation
                if (rowValues.length != columnCount) {
                    throw new IllegalArgumentException("Nombre de valeurs incorrect, attendu: " + 
                        columnCount + ", obtenu: " + rowValues.length);
                }
                
                        // Process all columns for this row
                        for (int colIdx = 0; colIdx < columnCount; colIdx++) {
                            Object value = rowValues[colIdx];
                            ColumnStore columnStore = columnStores[colIdx];
                    
                    if (value == null) {
                        columnStore.addNull();
                        continue;
                    }
                    
                            // Optimized type handling with fewer instanceof checks
                    switch (columnStore.getType()) {
                        case INTEGER:
                                    columnStore.addInt(value instanceof Integer ? 
                                        (Integer) value : ((Number) value).intValue());
                            break;
                        case LONG:
                                    columnStore.addLong(value instanceof Long ? 
                                        (Long) value : ((Number) value).longValue());
                            break;
                        case FLOAT:
                                    columnStore.addFloat(value instanceof Float ? 
                                        (Float) value : ((Number) value).floatValue());
                            break;
                        case DOUBLE:
                                    columnStore.addDouble(value instanceof Double ? 
                                        (Double) value : ((Number) value).doubleValue());
                            break;
                        case BOOLEAN:
                                    columnStore.addBoolean(value instanceof Boolean ? 
                                        (Boolean) value : Boolean.parseBoolean(value.toString()));
                            break;
                        case STRING:
                                    // Avoid string interning for better memory performance
                                    columnStore.addString(value instanceof String ? 
                                        (String) value : value.toString());
                            break;
                        case DATE:
                        case TIMESTAMP:
                                    columnStore.addDate(value instanceof Long ? 
                                        (Long) value : ((Number) value).longValue());
                            break;
                        default:
                            throw new IllegalArgumentException("Type non supporté: " + columnStore.getType());
                    }
                }
            }
                    
                    // Bulk increment row count - much faster than individual increments
                    tableData.incrementRowCount(batchRowCount);
            
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
                    // Avoid string interning for better performance - let GC handle duplicates
                    stringValues[i] = binary.toStringUsingUTF8();
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
            
            // Use reusable buffer for better memory efficiency
            byte[] buffer = bufferCache.get();
            long totalBytes = 0;
            
            try (BufferedInputStream bis = new BufferedInputStream(inputStream, ULTRA_BUFFER_SIZE);
                 FileOutputStream fileOS = new FileOutputStream(tempFile)) {
                
                int bytesRead;
                while ((bytesRead = bis.read(buffer)) != -1) {
                    fileOS.write(buffer, 0, bytesRead);
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
            
            // Use reusable buffer for efficient transfer
            byte[] buffer = bufferCache.get();
            try (BufferedInputStream bis = new BufferedInputStream(inputStream, ULTRA_BUFFER_SIZE);
                 FileOutputStream fileOS = new FileOutputStream(tempFile)) {
                
                int bytesRead;
                while ((bytesRead = bis.read(buffer)) != -1) {
                    fileOS.write(buffer, 0, bytesRead);
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
        
        final ParquetLoadStats stats = new ParquetLoadStats();
        final long startTime = System.currentTimeMillis();
        
        // Use reusable buffer for efficient streaming
        byte[] reusableBuffer = bufferCache.get();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4 * 1024 * 1024); // Start with 4MB
        long localBytesTransferred = 0;
        
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream, ULTRA_BUFFER_SIZE)) {
            int bytesRead;
            while ((bytesRead = bufferedInputStream.read(reusableBuffer)) != -1) {
                baos.write(reusableBuffer, 0, bytesRead);
                localBytesTransferred += bytesRead;
            }
        }
        logger.info("[{}] Flux Parquet lu en mémoire: {} MB",
                   sessionId, localBytesTransferred / (1024 * 1024));

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
            // Write to a temporary file with optimized I/O
            tempParquetFile = Files.createTempFile("memorydb-parquet-" + sessionId + "-", ".parquet");
            tempParquetFile.toFile().deleteOnExit();
            
            // Write directly from ByteBuffer to file with minimal copying
            try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(tempParquetFile, 
                    java.nio.file.StandardOpenOption.WRITE)) {
                channel.write(parquetByteBuffer);
            }
            
            // Use standard Hadoop Path API
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tempParquetFile.toUri());
            
            // Use modern ParquetFileReader API
            try (ParquetFileReader schemaReader = ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, conf))) {
            MessageType schema = schemaReader.getFooter().getFileMetaData().getSchema();
            validateSchema(table, schema);
            
                            // ULTRA-AGGRESSIVE: Massive batch sizes for maximum throughput
                int batchSize = Math.max(options.getBatchSize(), ULTRA_BATCH_SIZE); // Minimum 2M batch size
            int skipRows = options.getSkipRows();
            long rowLimit = options.getRowLimit();
            
            // Use the standard builder pattern
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), hadoopPath)
                    .withConf(conf)
                    .build()) {
                
                // Skip initial rows if needed
                Group record = null;
                for (int i = 0; i < skipRows && reader.read() != null; i++) {
                    // Skipping
                }
                
                // ULTRA-AGGRESSIVE: Initialize massive parallel processing
                int nodeIndex = 0;
                List<Object[]> batch = new ArrayList<>(batchSize);
                List<Column> columns = table.getColumns();
                
                // Make sure we properly distribute data across all nodes
                int nodeCount = nodes.length;
                logger.info("[{}] ULTRA-AGGRESSIVE: Configuré pour distribuer les données entre {} nœuds avec batches de {} lignes", 
                           sessionId, nodeCount, batchSize);
                
                long currentRow = 0;
                int currentRowsInBatch = 0;
                Map<String, Long> nodeRows = new HashMap<>();
                
                // Initialize the nodeRows map for proper distribution tracking
                for (com.memorydb.distribution.NodeInfo node : nodes) {
                    nodeRows.put(node.getId(), 0L);
                    logger.info("[{}] Initialisation du compteur pour le nœud {}", sessionId, node.getId());
                }
                
                // ULTRA-AGGRESSIVE: Pre-allocate massive arrays for vectorized processing
                Object[][] megaBatch = new Object[MEGA_BATCH_SIZE][];
                int megaBatchIndex = 0;
                                
                // Process all rows or up to row limit
                while ((record = reader.read()) != null && 
                       (rowLimit <= 0 || currentRow < rowLimit)) {
                        
                        // Get node for this row (round-robin)
                        int actualNodeIndex = (int)(currentRow % nodes.length);
                        com.memorydb.distribution.NodeInfo currentNode = nodes[actualNodeIndex];
                        String nodeId = currentNode.getId();
                        
                        if (currentRow % 500000 == 0) {
                            logger.info("[{}] Ligne {} attribuée au nœud {} (index {})", 
                                sessionId, currentRow, nodeId, actualNodeIndex);
                        }
                        
                        // Extract row values and add to batch
                        Object[] rowValues = extractValues(record, columns, schema);
                        batch.add(rowValues);
                        currentRowsInBatch++;
                        
                        // ULTRA-AGGRESSIVE: Process mega-batches with parallel streaming
                        megaBatch[megaBatchIndex++] = rowValues;
                        
                        // Process batch if full
                        if (currentRowsInBatch >= batchSize || megaBatchIndex >= MEGA_BATCH_SIZE) {
                            // Si c'est le nœud local, ajouter directement à la table locale
                            final String localNodeId = clusterManager.getLocalNode().getId();
                            final String finalNodeId = nodeId;
                            final com.memorydb.distribution.NodeInfo finalNode = currentNode;
                            final List<Object[]> batchToProcess = new ArrayList<>(batch); // Create a copy to avoid concurrent modification
                            final int finalBatchSize = currentRowsInBatch;
                            
                            if (nodeId.equals(localNodeId)) {
                                // ULTRA-AGGRESSIVE: Async local processing with dedicated thread pool
                                CompletableFuture<Void> localFuture = CompletableFuture.runAsync(() -> {
                                tableData.writeLock();
                                try {
                                    addBatchToTable(tableData, batchToProcess);
                                // Update stats immediately for local node
                                long previousCount = nodeRows.get(finalNodeId);
                                nodeRows.put(finalNodeId, previousCount + finalBatchSize);
                                stats.addNodeRows(finalNodeId, finalBatchSize);
                                        totalRowsProcessed.addAndGet(finalBatchSize);
                                    } finally {
                                        tableData.writeUnlock();
                                    }
                                }, processingExecutorService);
                                
                                pendingOperations.add(localFuture);
                            } else {
                                // ULTRA-AGGRESSIVE: Async remote processing with persistent HTTP clients
                                CompletableFuture<Void> remoteFuture = CompletableFuture.runAsync(() -> {
                                    boolean success = false;
                                    int retryCount = 0;
                                    int maxRetries = 2; // Reduced retries for speed
                                    
                                    while (!success && retryCount < maxRetries) {
                                        try {
                                            sendBatchToRemoteNodeUltraFast(finalNode, tableName, batchToProcess);
                                            success = true;
                                        
                                        // Update stats after successful send
                                        synchronized (nodeRows) {
                                            long previousCount = nodeRows.get(finalNodeId);
                                            nodeRows.put(finalNodeId, previousCount + finalBatchSize);
                                            stats.addNodeRows(finalNodeId, finalBatchSize);
                                                totalRowsProcessed.addAndGet(finalBatchSize);
                                                networkOperations.incrementAndGet();
                                        }
                                    } catch (Exception e) {
                                            retryCount++;
                                            if (retryCount >= maxRetries) {
                                                logger.error("[{}] ULTRA-FAST: Échec définitif de l'envoi au nœud {} après {} tentatives: {}", 
                                                        sessionId, finalNodeId, maxRetries, e.getMessage());
                                                
                                                // ULTRA-AGGRESSIVE: Fast fallback to local node
                                                try {
                                                    tableData.writeLock();
                                                    try {
                                                        addBatchToTable(tableData, batchToProcess);
                                                        synchronized (nodeRows) {
                                                            long previousCount = nodeRows.get(finalNodeId);
                                                            nodeRows.put(finalNodeId, previousCount + finalBatchSize);
                                                            stats.addNodeRows(finalNodeId, finalBatchSize);
                                                            totalRowsProcessed.addAndGet(finalBatchSize);
                                                        }
                                                    } finally {
                                                        tableData.writeUnlock();
                                                    }
                                                } catch (Exception fallbackError) {
                                                    logger.error("[{}] ULTRA-FAST: Échec du fallback local: {}", sessionId, fallbackError.getMessage());
                                    }
                                            } else {
                                                // Minimal retry delay for speed
                                                try {
                                                    Thread.sleep(100 * retryCount); // Much faster retry
                                                } catch (InterruptedException ie) {
                                                    Thread.currentThread().interrupt();
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }, networkExecutorService);
                                
                                pendingOperations.add(remoteFuture);
                            }
                            
                            batch.clear();
                            currentRowsInBatch = 0;
                            megaBatchIndex = 0; // Reset mega batch
                            
                            // ULTRA-AGGRESSIVE: Less frequent logging for better performance
                            if (currentRow % 2000000 == 0) { // Log every 2M rows
                                long totalProcessed = totalRowsProcessed.get();
                                long networkOps = networkOperations.get();
                                logger.info("[{}] ULTRA-FAST Progress: {} rows read, {} rows processed, {} network ops", 
                                           sessionId, currentRow, totalProcessed, networkOps);
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
                            // For remote nodes, send final batch with retry logic
                            boolean success = false;
                            int retryCount = 0;
                            int maxRetries = 3;
                            
                            while (!success && retryCount < maxRetries) {
                                try {
                                    sendBatchToRemoteNode(finalBatchNode, tableName, finalBatch);
                                    success = true;
                                    
                                    // Update stats after successful send
                                        long prevCount = nodeRows.get(finalBatchNodeId);
                                        nodeRows.put(finalBatchNodeId, prevCount + finalBatchSize);
                                        stats.addNodeRows(finalBatchNodeId, finalBatchSize);
                                } catch (Exception e) {
                                    retryCount++;
                                    if (retryCount >= maxRetries) {
                                        logger.error("[{}] Échec définitif du batch final au nœud {} après {} tentatives: {}", 
                                                 sessionId, finalBatchNodeId, maxRetries, e.getMessage());
                                        
                                        // Fallback: add final batch to local node
                                        try {
                                            logger.info("[{}] Fallback: ajout du batch final au nœud local", sessionId);
                                            tableData.writeLock();
                                            try {
                                                addBatchToTable(tableData, finalBatch);
                                                // Update stats for local fallback
                                                long prevCount = nodeRows.get(finalBatchNodeId);
                                                nodeRows.put(finalBatchNodeId, prevCount + finalBatchSize);
                                                stats.addNodeRows(finalBatchNodeId, finalBatchSize);
                                            } finally {
                                                tableData.writeUnlock();
                                            }
                                        } catch (Exception fallbackError) {
                                            logger.error("[{}] Échec du fallback local pour le batch final: {}", sessionId, fallbackError.getMessage());
                                }
                                    } else {
                                        logger.warn("[{}] Tentative {}/{} échouée pour le batch final au nœud {}, retry...", 
                                                sessionId, retryCount, maxRetries, finalBatchNodeId);
                                        try {
                                            Thread.sleep(1000 * retryCount); // Exponential backoff
                                        } catch (InterruptedException ie) {
                                            Thread.currentThread().interrupt();
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // Update final stats
                    stats.setElapsedTimeMs(System.currentTimeMillis() - startTime);
                    long totalDistributed = nodeRows.values().stream().mapToLong(Long::longValue).sum();
                    
                    logger.info("[{}] Chargement distribué terminé: {} lignes lues, {} lignes distribuées en {} ms", 
                              sessionId, currentRow, totalDistributed, stats.getElapsedTimeMs());
                    logger.info("[{}] Distribution finale par nœud: {}", sessionId, nodeRows);
                }
            }
            
            // ULTRA-AGGRESSIVE: Wait for all async operations to complete
            logger.info("[{}] ULTRA-FAST: Waiting for {} async operations to complete...", 
                       sessionId, pendingOperations.size());
            
            CompletableFuture<Void> allOperations = CompletableFuture.allOf(
                pendingOperations.toArray(new CompletableFuture[0])
            );
            
            try {
                // Wait with timeout for all operations
                allOperations.get(5, java.util.concurrent.TimeUnit.MINUTES);
                logger.info("[{}] ULTRA-FAST: All async operations completed successfully", sessionId);
                } catch (Exception e) {
                logger.error("[{}] ULTRA-FAST: Some async operations failed: {}", sessionId, e.getMessage());
            }
            
            // Final performance metrics
            long finalProcessed = totalRowsProcessed.get();
            long finalBytes = totalBytesTransferred.get();
            long finalNetworkOps = networkOperations.get();
            
            // Update final elapsed time
            stats.setElapsedTimeMs(System.currentTimeMillis() - startTime);
            
            logger.info("[{}] ULTRA-FAST FINAL STATS: {} rows processed, {} bytes transferred, {} network ops, {} ms total", 
                       sessionId, finalProcessed, finalBytes, finalNetworkOps, stats.getElapsedTimeMs());
            
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
     * ULTRA-AGGRESSIVE: Ultra-fast binary batch sender with persistent HTTP clients
     * This method uses connection pooling and minimal error handling for maximum speed
     * 
     * @param node Nœud distant où envoyer les données
     * @param tableName Nom de la table à mettre à jour
     * @param batch Liste d'objets représentant les lignes à ajouter
     * @throws IOException En cas d'erreur d'E/S ou de communication
     */
    private void sendBatchToRemoteNodeUltraFast(com.memorydb.distribution.NodeInfo node, String tableName, 
                                               List<Object[]> batch) throws IOException {
        try {
            if (batch.isEmpty()) {
                return; // Rien à envoyer
            }
            
            int rowCount = batch.size();
            int columnCount = batch.get(0).length;
            
            // ULTRA-AGGRESSIVE: Create binary payload using direct ByteBuffer for zero-copy operations
            ByteBuffer binaryPayload = createBinaryBatchPayload(tableName, batch, rowCount, columnCount);
            
            // Construction de l'URL du nœud distant - use new binary endpoint
            String url = String.format("http://%s:%d/api/tables/%s/add-batch-binary", 
                    node.getAddress(), node.getPort(), tableName);
                
            // ULTRA-AGGRESSIVE: Use persistent HTTP client from pool
            HttpClient persistentClient = getHttpClientForNode(node.getId());
            
            // ULTRA-AGGRESSIVE: Minimal headers and faster timeout
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("Content-Type", "application/octet-stream")
                    .header("X-Row-Count", String.valueOf(rowCount))
                    .header("X-Column-Count", String.valueOf(columnCount))
                    .timeout(java.time.Duration.ofSeconds(30)) // Faster timeout
                    .POST(HttpRequest.BodyPublishers.ofByteArray(binaryPayload.array()))
                    .build();
            
            // ULTRA-AGGRESSIVE: Send with minimal error handling for speed
            HttpResponse<String> response = persistentClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            // ULTRA-AGGRESSIVE: Fast error checking
            if (response.statusCode() != 200) {
                throw new IOException("HTTP " + response.statusCode() + " from node " + node.getId());
            }
            
            // Update performance metrics
            totalBytesTransferred.addAndGet(binaryPayload.capacity());
            
            logger.debug("ULTRA-FAST: {} rows sent to {} ({} bytes)", 
                    rowCount, node.getId(), binaryPayload.capacity());
            
        } catch (URISyntaxException | InterruptedException e) {
            throw new IOException("ULTRA-FAST communication error with node " + node.getId() + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Ultra-fast binary batch sender using direct ByteBuffers and zero-copy operations
     * This method bypasses JSON serialization entirely for maximum performance
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
            
            // Create binary payload using direct ByteBuffer for zero-copy operations
            ByteBuffer binaryPayload = createBinaryBatchPayload(tableName, batch, rowCount, columnCount);
            
            // Construction de l'URL du nœud distant - use new binary endpoint
            String url = String.format("http://%s:%d/api/tables/%s/add-batch-binary", 
                    node.getAddress(), node.getPort(), tableName);
                
            // Create a fresh HTTP client for each request to avoid connection reuse issues
            HttpClient freshClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1) // Use HTTP/1.1 for better stability
                .connectTimeout(java.time.Duration.ofSeconds(30))
                .build();
            
            // Préparation de la requête HTTP avec payload binaire
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("Content-Type", "application/octet-stream")
                    .header("X-Row-Count", String.valueOf(rowCount))
                    .header("X-Column-Count", String.valueOf(columnCount))
                    .timeout(java.time.Duration.ofMinutes(5)) // Longer timeout for large batches
                    .POST(HttpRequest.BodyPublishers.ofByteArray(binaryPayload.array()))
                    .build();
            
            // Envoi de la requête avec gestion d'erreur améliorée
            HttpResponse<String> response;
            try {
                response = freshClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (java.net.ConnectException e) {
                throw new IOException("Impossible de se connecter au nœud " + node.getId() + 
                        " à l'adresse " + node.getAddress() + ":" + node.getPort(), e);
            } catch (java.net.SocketTimeoutException e) {
                throw new IOException("Timeout lors de l'envoi au nœud " + node.getId(), e);
            }
            
            // Vérification de la réponse
            if (response.statusCode() != 200) {
                String errorMsg = "Erreur HTTP " + response.statusCode() + " du nœud " + node.getId();
                if (response.body() != null && !response.body().isEmpty()) {
                    errorMsg += " - " + response.body();
                }
                throw new IOException(errorMsg);
            }
            
            logger.debug("Batch binaire de {} lignes envoyé au nœud {} ({} bytes)", 
                    rowCount, node.getId(), binaryPayload.capacity());
            
        } catch (URISyntaxException | InterruptedException e) {
            throw new IOException("Erreur lors de la communication avec le nœud distant: " + e.getMessage(), e);
        }
    }
    
    /**
     * Ultra-fast Kryo serialization for complex objects
     */
    private byte[] serializeWithKryo(Object obj) {
        Kryo kryo = kryoCache.get();
        try (Output output = new Output(4096, -1)) {
            kryo.writeObject(output, obj);
            return output.toBytes();
        }
    }
    
    /**
     * Ultra-fast compression using DEFLATE with optimized settings
     */
    private byte[] compressData(byte[] data, int offset, int length) throws IOException {
        Deflater deflater = new Deflater(Deflater.BEST_SPEED); // Prioritize speed over compression ratio
        deflater.setStrategy(Deflater.HUFFMAN_ONLY); // Faster compression strategy
        
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(length / 2);
             DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater, 8192)) {
            
            dos.write(data, offset, length);
            dos.finish();
            return baos.toByteArray();
        } finally {
            deflater.end();
        }
    }
    
    /**
     * Calculates the exact size needed for the binary payload to avoid buffer overflow
     */
    private int calculatePayloadSize(String tableName, List<Object[]> batch, int rowCount, int columnCount) {
        int size = 0;
        
        // Table name: 4 bytes (length) + actual bytes
        size += 4 + tableName.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        
        // Dimensions: 4 bytes each for rowCount and columnCount
        size += 8;
        
        // Column types: 1 byte per column
        size += columnCount;
        
        // For each column, calculate data size
        for (int col = 0; col < columnCount; col++) {
            byte columnType = detectColumnType(batch, col);
            
            // Null bitmap: (rowCount + 7) / 8 bytes
            size += (rowCount + 7) / 8;
            
            switch (columnType) {
                case 1: // INT
                    // Count non-null values only
                    for (int row = 0; row < rowCount; row++) {
                        if (batch.get(row)[col] != null) {
                            size += 4; // 4 bytes per int
                        }
                    }
                        break;
                case 2: // LONG
                    // Count non-null values only
                    for (int row = 0; row < rowCount; row++) {
                        if (batch.get(row)[col] != null) {
                            size += 8; // 8 bytes per long
                            }
                        }
                        break;
                case 3: // FLOAT
                    // Count non-null values only
                    for (int row = 0; row < rowCount; row++) {
                        if (batch.get(row)[col] != null) {
                            size += 4; // 4 bytes per float
                            }
                        }
                        break;
                case 4: // DOUBLE
                    // Count non-null values only
                    for (int row = 0; row < rowCount; row++) {
                        if (batch.get(row)[col] != null) {
                            size += 8; // 8 bytes per double
                        }
                    }
                    break;
                case 5: // BOOLEAN
                    size += (rowCount + 7) / 8; // Packed booleans (all rows, null handled by bitmap)
                    break;
                case 6: // STRING
                case 7: // OTHER (converted to string)
                    default:
                    // For strings: 4 bytes (length) + actual string bytes for each non-null row
                    for (int row = 0; row < rowCount; row++) {
                        Object value = batch.get(row)[col];
                        if (value != null) {
                            size += 4; // Length prefix
                            String strValue = value.toString();
                            size += strValue.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                        }
                    }
                        break;
                }
        }
        
        return size;
            }
            
    /**
     * Creates an ultra-compact binary payload using direct ByteBuffer with compression
     * Format: [compressed_flag][tableName_length][tableName][rowCount][columnCount][type_info][data_blocks]
     * This format is designed for maximum speed and minimal memory allocation
     */
    private ByteBuffer createBinaryBatchPayload(String tableName, List<Object[]> batch, int rowCount, int columnCount) {
        // Create uncompressed payload first
        ByteBuffer uncompressedBuffer = createUncompressedPayload(tableName, batch, rowCount, columnCount);
        
        // Try compression for large payloads (>64KB)
        if (uncompressedBuffer.remaining() > 65536) {
            try {
                byte[] compressed = compressData(uncompressedBuffer.array(), 0, uncompressedBuffer.remaining());
                
                // Only use compression if it saves significant space (>20% reduction)
                if (compressed.length < uncompressedBuffer.remaining() * 0.8) {
                    ByteBuffer compressedBuffer = ByteBuffer.allocate(compressed.length + 1);
                    compressedBuffer.put((byte) 1); // Compression flag
                    compressedBuffer.put(compressed);
                    compressedBuffer.flip();
                    return compressedBuffer;
                }
            } catch (Exception e) {
                logger.debug("Compression failed, using uncompressed data: {}", e.getMessage());
            }
        }
        
        // Use uncompressed data
        ByteBuffer finalBuffer = ByteBuffer.allocate(uncompressedBuffer.remaining() + 1);
        finalBuffer.put((byte) 0); // No compression flag
        finalBuffer.put(uncompressedBuffer);
        finalBuffer.flip();
        return finalBuffer;
    }
    
    /**
     * Creates the uncompressed binary payload
     */
    private ByteBuffer createUncompressedPayload(String tableName, List<Object[]> batch, int rowCount, int columnCount) {
        // Calculate actual size needed to avoid buffer overflow
        int actualSize = calculatePayloadSize(tableName, batch, rowCount, columnCount);
        // Add 10% safety margin (reduced from 20% since we're more accurate now)
        int bufferSize = (int) (actualSize * 1.1) + 512;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize); // Use heap buffer for easier array access
        
        // Write table name
        byte[] tableNameBytes = tableName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        buffer.putInt(tableNameBytes.length);
        buffer.put(tableNameBytes);
            
        // Write dimensions
        buffer.putInt(rowCount);
        buffer.putInt(columnCount);
        
        // Analyze column types from first non-null row
        byte[] columnTypes = new byte[columnCount];
        for (int col = 0; col < columnCount; col++) {
            columnTypes[col] = detectColumnType(batch, col);
        }
        buffer.put(columnTypes);
        
        // Write data in column-major order for better cache locality
        for (int col = 0; col < columnCount; col++) {
            writeColumnData(buffer, batch, col, rowCount, columnTypes[col]);
        }
        
        // Flip buffer for reading
        buffer.flip();
        return buffer;
    }
    
    /**
     * Detects the type of a column by examining the first non-null value
     * Also considers Number types that might be boxed differently
     */
    private byte detectColumnType(List<Object[]> batch, int columnIndex) {
        for (Object[] row : batch) {
            Object value = row[columnIndex];
            if (value != null) {
                if (value instanceof Integer) return 1; // INT
                if (value instanceof Long) return 2; // LONG
                if (value instanceof Float) return 3; // FLOAT
                if (value instanceof Double) return 4; // DOUBLE
                if (value instanceof Boolean) return 5; // BOOLEAN
                if (value instanceof String) return 6; // STRING
                // Handle other Number types
                if (value instanceof Number) {
                    Number num = (Number) value;
                    // Try to determine the best fit
                    if (num.doubleValue() == num.longValue()) {
                        return 2; // LONG
                    } else {
                        return 4; // DOUBLE
                    }
                }
                return 7; // OTHER (will be converted to string)
            }
        }
        return 0; // NULL_ONLY
    }
    
    /**
     * Writes column data in the most efficient binary format with vectorized operations
     */
    private void writeColumnData(ByteBuffer buffer, List<Object[]> batch, int columnIndex, int rowCount, byte columnType) {
        // Pre-allocate arrays for vectorized processing
        Object[] columnValues = new Object[rowCount];
        boolean[] nulls = new boolean[rowCount];
            
        // Extract column data in one pass for better cache locality
        for (int row = 0; row < rowCount; row++) {
            Object value = batch.get(row)[columnIndex];
            columnValues[row] = value;
            nulls[row] = (value == null);
        }
        
        // Write null bitmap (1 bit per row, packed into bytes)
        byte[] nullBitmap = new byte[(rowCount + 7) / 8];
        for (int row = 0; row < rowCount; row++) {
            if (nulls[row]) {
                int byteIndex = row / 8;
                int bitIndex = row % 8;
                nullBitmap[byteIndex] |= (1 << bitIndex);
            }
        }
        buffer.put(nullBitmap);
        
        // Write actual data based on type (only for non-null values) - vectorized
        switch (columnType) {
            case 1: // INT - vectorized processing
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        buffer.putInt(((Number) columnValues[row]).intValue());
                    }
                }
                break;
            case 2: // LONG - vectorized processing
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        buffer.putLong(((Number) columnValues[row]).longValue());
            }
                }
                break;
            case 3: // FLOAT - vectorized processing
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        buffer.putFloat(((Number) columnValues[row]).floatValue());
                    }
                }
                break;
            case 4: // DOUBLE - vectorized processing
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        buffer.putDouble(((Number) columnValues[row]).doubleValue());
                    }
                }
                break;
            case 5: // BOOLEAN - vectorized processing
                // Pack booleans into bits for maximum efficiency (only non-null values)
                byte[] boolData = new byte[(rowCount + 7) / 8];
                for (int row = 0; row < rowCount; row++) {
                    // Only process non-null values
                    if (!nulls[row] && ((Boolean) columnValues[row])) {
                        int byteIndex = row / 8;
                        int bitIndex = row % 8;
                        boolData[byteIndex] |= (1 << bitIndex);
                    }
                    // Null values are handled by the null bitmap, false values remain 0 in boolData
                }
                buffer.put(boolData);
                break;
            case 6: // STRING - vectorized processing with string interning for duplicates
                // Pre-process strings for potential optimization
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        String strValue = columnValues[row].toString();
                        byte[] strBytes = strValue.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        buffer.putInt(strBytes.length);
                        buffer.put(strBytes);
        }
    }
                break;
            default:
                // For unknown types, convert to string (only for non-null values) - vectorized
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        String strValue = columnValues[row].toString();
                        byte[] strBytes = strValue.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        buffer.putInt(strBytes.length);
                        buffer.put(strBytes);
                    }
                }
                break;
        }
    }

    public void shutdown() {
        if (ultraExecutorService != null && !ultraExecutorService.isShutdown()) {
            ultraExecutorService.shutdown();
        }
        
        if (networkExecutorService != null && !networkExecutorService.isShutdown()) {
            networkExecutorService.shutdown();
        }
        
        if (processingExecutorService != null && !processingExecutorService.isShutdown()) {
            processingExecutorService.shutdown();
        }
        
        // Clear thread-local caches to help with memory cleanup
        valueCache.remove();
        bufferCache.remove();
    }
}
