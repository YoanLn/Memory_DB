package com.memorydb.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Utilitaire simplifié pour la lecture de schémas de fichiers Parquet
 */
public class ParquetSchemaReader {
    private static final Logger logger = LoggerFactory.getLogger(ParquetSchemaReader.class);
    
    /**
     * Lit et affiche le schéma d'un fichier Parquet
     * @param filePath Le chemin du fichier
     * @return Le schéma, ou null en cas d'erreur
     */
    public static MessageType readSchema(String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                logger.error("Le fichier n'existe pas: {}", filePath);
                return null;
            }
            
            Configuration conf = new Configuration();
            // Désactiver l'utilisation du SecurityManager
            conf.set("hadoop.security.authentication", "simple");
            conf.set("hadoop.security.authorization", "false");
            
            Path path = new Path(file.getAbsolutePath());
            ParquetFileReader reader = ParquetFileReader.open(
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, conf));
            
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            reader.close();
            
            // Affiche le schéma
            logger.info("Schéma du fichier {}: {}", filePath, schema);
            List<Type> fields = schema.getFields();
            for (Type field : fields) {
                logger.info("  Champ: {} ({})", field.getName(), field.asPrimitiveType().getPrimitiveTypeName());
            }
            
            return schema;
        } catch (IOException e) {
            logger.error("Erreur lors de la lecture du fichier Parquet: {}", e.getMessage(), e);
            return null;
        }
    }
} 