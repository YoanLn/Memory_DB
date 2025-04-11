package com.memorydb.parquet;

import com.memorydb.common.DataType;
import com.memorydb.core.Column;
import com.memorydb.core.DatabaseContext;
import com.memorydb.core.Table;
import com.memorydb.storage.TableData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Chargeur de fichiers Parquet
 */
@ApplicationScoped
public class ParquetLoader {
    
    @Inject
    private DatabaseContext databaseContext;
    
    /**
     * Charge un fichier Parquet dans une table existante
     * @param tableName Le nom de la table
     * @param filePath Le chemin du fichier Parquet
     * @return Le nombre de lignes chargées
     */
    public int loadParquetFile(String tableName, String filePath) throws IOException {
        return loadParquetFile(tableName, filePath, -1); // -1 signifie pas de limite
    }
    
    /**
     * Charge un fichier Parquet dans une table existante avec une limite de lignes
     * @param tableName Le nom de la table
     * @param filePath Le chemin du fichier Parquet
     * @param maxRows Le nombre maximum de lignes à charger (-1 pour aucune limite)
     * @return Le nombre de lignes chargées
     */
    public int loadParquetFile(String tableName, String filePath, int maxRows) throws IOException {
        // Vérifie que la table existe
        Table table = databaseContext.getTable(tableName);
        TableData tableData = databaseContext.getTableData(tableName);
        
        // Ouvre le fichier Parquet
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        
        AtomicInteger rowCount = new AtomicInteger(0);
        
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            
            // Vérifie la compatibilité des schémas
            validateSchema(table, schema);
            
            // Lecture des données par bloc
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rowGroupRowCount = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(
                    pages, new GroupRecordConverter(schema));
                
                // Traitement des lignes
                for (int i = 0; i < rowGroupRowCount; i++) {
                    // Vérifie si on a atteint la limite de lignes
                    if (maxRows > 0 && rowCount.get() >= maxRows) {
                        break;
                    }
                    
                    Group group = recordReader.read();
                    Object[] values = extractValues(group, table, schema);
                    tableData.addRow(values);
                    rowCount.incrementAndGet();
                }
                
                // Vérifie encore une fois si on a atteint la limite après avoir traité le bloc
                if (maxRows > 0 && rowCount.get() >= maxRows) {
                    break;
                }
            }
        }
        
        return rowCount.get();
    }
    
    /**
     * Crée une table à partir d'un fichier Parquet
     * @param tableName Le nom de la table à créer
     * @param filePath Le chemin du fichier Parquet
     * @return La table créée
     */
    public Table createTableFromParquet(String tableName, String filePath) throws IOException {
        return createTableFromParquet(tableName, filePath, -1); // -1 signifie pas de limite
    }
    
    /**
     * Crée une table à partir d'un fichier Parquet avec une limite de lignes
     * @param tableName Le nom de la table à créer
     * @param filePath Le chemin du fichier Parquet
     * @param maxRows Le nombre maximum de lignes à charger (-1 pour aucune limite)
     * @return La table créée
     */
    public Table createTableFromParquet(String tableName, String filePath, int maxRows) throws IOException {
        return createTableFromParquet(tableName, filePath, maxRows, true);
    }
    
    /**
     * Crée une table à partir d'un fichier Parquet avec options
     * @param tableName Le nom de la table à créer
     * @param filePath Le chemin du fichier Parquet
     * @param maxRows Le nombre maximum de lignes à charger (-1 pour aucune limite)
     * @param loadData Indique si les données doivent être chargées
     * @return La table créée
     */
    public Table createTableFromParquet(String tableName, String filePath, int maxRows, boolean loadData) throws IOException {
        // Vérifie que la table n'existe pas déjà
        if (databaseContext.tableExists(tableName)) {
            throw new IllegalStateException("La table existe déjà: " + tableName);
        }
        
        // Ouvre le fichier Parquet pour lire le schéma
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
            
            // Convertit le schéma Parquet en colonnes
            List<Column> columns = convertSchema(parquetSchema);
            
            // Crée la table
            Table table = databaseContext.createTable(tableName, columns);
            
            // Charge les données seulement si demandé
            if (loadData) {
                loadParquetFile(tableName, filePath, maxRows);
            }
            
            return table;
        }
    }
    
    /**
     * Crée uniquement le schéma d'une table à partir d'un fichier Parquet, sans charger les données
     * @param tableName Le nom de la table à créer
     * @param filePath Le chemin du fichier Parquet
     * @return La table créée (vide)
     */
    public Table createTableSchemaFromParquet(String tableName, String filePath) throws IOException {
        return createTableFromParquet(tableName, filePath, -1, false);
    }
    
    /**
     * Valide que le schéma Parquet est compatible avec la table
     * @param table La table
     * @param schema Le schéma Parquet
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
                    "': Table(" + column.getType() + ") vs Parquet(" + parquetField.asPrimitiveType().getPrimitiveTypeName() + ")");
            }
        }
    }
    
    /**
     * Convertit un schéma Parquet en liste de colonnes
     * @param schema Le schéma Parquet
     * @return La liste des colonnes
     */
    private List<Column> convertSchema(MessageType schema) {
        List<Column> columns = new ArrayList<>();
        List<Type> fields = schema.getFields();
        
        for (Type field : fields) {
            String name = field.getName();
            boolean nullable = field.isRepetition(Type.Repetition.OPTIONAL);
            DataType dataType = convertParquetType(field);
            
            columns.add(new Column(name, dataType, nullable));
        }
        
        return columns;
    }
    
    /**
     * Convertit un type Parquet en DataType
     * @param type Le type Parquet
     * @return Le DataType correspondant
     */
    private DataType convertParquetType(Type type) {
        if (!type.isPrimitive()) {
            throw new IllegalArgumentException("Les types complexes ne sont pas supportés: " + type);
        }
        
        PrimitiveType.PrimitiveTypeName typeName = type.asPrimitiveType().getPrimitiveTypeName();
        
        switch (typeName) {
            case INT32:
                return DataType.INTEGER;
            case INT64:
                return DataType.LONG;
            case FLOAT:
                return DataType.FLOAT;
            case DOUBLE:
                return DataType.DOUBLE;
            case BOOLEAN:
                return DataType.BOOLEAN;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return DataType.STRING;
            case INT96:
                return DataType.TIMESTAMP;
            default:
                throw new IllegalArgumentException("Type Parquet non supporté: " + typeName);
        }
    }
    
    /**
     * Vérifie si un type de colonne est compatible avec un type Parquet
     * @param columnType Le type de la colonne
     * @param parquetField Le champ Parquet
     * @return true si les types sont compatibles
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
                return typeName == PrimitiveType.PrimitiveTypeName.INT96;
            default:
                return false;
        }
    }
    
    /**
     * Extrait les valeurs d'un groupe Parquet
     * @param group Le groupe Parquet
     * @param table La table
     * @param schema Le schéma Parquet
     * @return Les valeurs extraites
     */
    private Object[] extractValues(Group group, Table table, MessageType schema) {
        List<Column> columns = table.getColumns();
        Object[] values = new Object[columns.size()];
        
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            Type field = schema.getType(i);
            String fieldName = field.getName();
            
            // Vérifie si la valeur est null
            if (column.isNullable() && field.isRepetition(Type.Repetition.OPTIONAL) && 
                group.getFieldRepetitionCount(i) == 0) {
                values[i] = null;
                continue;
            }
            
            // Extrait la valeur selon le type
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
                    values[i] = group.getBinary(fieldName, 0).toStringUsingUTF8();
                    break;
                case DATE:
                case TIMESTAMP:
                    values[i] = convertInt96ToTimestamp(group.getInt96(fieldName, 0));
                    break;
                default:
                    throw new IllegalArgumentException("Type non supporté: " + column.getType());
            }
        }
        
        return values;
    }
    
    /**
     * Convertit un INT96 en timestamp (millisecondes depuis l'epoch)
     * Note: Implémentation simplifiée, les INT96 dans Parquet sont plus complexes
     * @param int96 La valeur INT96
     * @return Le timestamp en millisecondes
     */
    private long convertInt96ToTimestamp(org.apache.parquet.io.api.Binary int96) {
        // Cette implémentation est simplifiée. 
        // L'implémentation réelle devrait convertir le INT96 en timestamp en tenant compte du format spécifique
        return 0; // À implémenter correctement
    }
} 