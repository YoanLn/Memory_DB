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
import java.io.IOException;
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
            long totalRows = 0;
            int batchCount = 0;
            boolean timeout = false;
            
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
                    while ((record = reader.read()) != null) {
                        // Vérifie le timeout
                        if (options.getTimeoutSeconds() > 0 && 
                            (System.currentTimeMillis() - startTime) / 1000 > options.getTimeoutSeconds()) {
                            logger.warn("Timeout atteint après {} secondes", options.getTimeoutSeconds());
                            timeout = true;
                            break;
                        }
                        
                        // Vérifie la limite de lignes
                        if (rowLimit > 0 && totalRows >= rowLimit) {
                            logger.info("Limite de lignes atteinte: {}", rowLimit);
                            break;
                        }
                        
                        // Extraction des valeurs de la ligne
                        Object[] rowValues = extractValues(record, columns, schema);
                        batchData.add(rowValues);
                        totalRows++;
                        
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
     * Ferme les ressources utilisées
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
}
