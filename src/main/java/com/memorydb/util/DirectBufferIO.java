package com.memorydb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

/**
 * Utility class for optimized I/O operations using direct ByteBuffers
 * Direct ByteBuffers allocate memory outside the JVM heap, which:
 * 1. Reduces garbage collection pressure
 * 2. Improves I/O performance for large files
 * 3. Better utilizes native I/O operations
 */
public class DirectBufferIO {
    private static final Logger logger = LoggerFactory.getLogger(DirectBufferIO.class);
    
    // Default buffer sizes
    private static final int DEFAULT_DIRECT_BUFFER_SIZE = 8 * 1024 * 1024; // 8 MB
    private static final int DEFAULT_TEMP_BUFFER_SIZE = 64 * 1024;         // 64 KB
    
    /**
     * Saves an InputStream to a temporary file using direct ByteBuffers for optimal performance
     * @param inputStream The input stream to save
     * @param prefix File name prefix (optional)
     * @param suffix File extension (optional)
     * @return The created temporary file
     * @throws IOException If an I/O error occurs
     */
    public static File saveToTempFile(InputStream inputStream, String prefix, String suffix) throws IOException {
        String sessionId = UUID.randomUUID().toString();
        String filePrefix = prefix != null ? prefix : "file_upload_";
        String fileSuffix = suffix != null ? suffix : ".tmp";
        
        logger.info("[{}] Starting optimized file upload with direct ByteBuffer", sessionId);
        
        // Validate input
        if (inputStream == null) {
            throw new IOException("InputStream is null");
        }
        
        // Create temp file
        File tempFile = File.createTempFile(filePrefix, fileSuffix);
        logger.info("[{}] Temporary file created: {}", sessionId, tempFile.getAbsolutePath());
        
        // Ensure file permissions
        tempFile.setReadable(true, false);
        tempFile.setWritable(true, false);
        
        // Transfer with direct buffer
        long totalBytes = 0;
        long startTime = System.currentTimeMillis();
        
        try {
            totalBytes = transferToFileWithDirectBuffer(
                inputStream, 
                tempFile, 
                DEFAULT_DIRECT_BUFFER_SIZE,
                DEFAULT_TEMP_BUFFER_SIZE,
                sessionId
            );
            
            double mbTransferred = totalBytes / (1024.0 * 1024.0);
            double elapsedSecs = (System.currentTimeMillis() - startTime) / 1000.0;
            double mbPerSec = elapsedSecs > 0 ? mbTransferred / elapsedSecs : 0;
            
            logger.info("[{}] File transfer complete: {:.2f} MB in {:.2f} sec ({:.2f} MB/sec)",
                    sessionId, mbTransferred, elapsedSecs, mbPerSec);
            
            // Verify basic Parquet format if it's a Parquet file
            if (fileSuffix.toLowerCase().endsWith(".parquet")) {
                verifyParquetHeader(tempFile, sessionId);
            }
            
            return tempFile;
        } catch (IOException e) {
            // Clean up on failure
            logger.error("[{}] Error saving file: {}", sessionId, e.getMessage());
            if (tempFile.exists()) {
                tempFile.delete();
            }
            throw e;
        }
    }
    
    /**
     * Transfer data from an InputStream to a file using direct ByteBuffer for improved performance
     * @param inputStream Source input stream
     * @param outputFile Destination file
     * @param directBufferSize Size of the direct buffer (in bytes)
     * @param tempBufferSize Size of the temporary buffer for reading from InputStream
     * @param sessionId Session ID for logging
     * @return Total bytes transferred
     * @throws IOException If an I/O error occurs
     */
    public static long transferToFileWithDirectBuffer(
            InputStream inputStream, 
            File outputFile,
            int directBufferSize,
            int tempBufferSize,
            String sessionId) throws IOException {
        
        long totalBytes = 0;
        long startTime = System.currentTimeMillis();
        
        // IMPORTANT: We don't close the inputStream here because it's provided by the caller
        // and they are responsible for closing it
        
        // Use try-with-resources to ensure resources are closed properly
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             FileChannel fileChannel = fos.getChannel()) {
            // Allocate a direct ByteBuffer (outside the Java heap)
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(directBufferSize);
            byte[] tempBuffer = new byte[tempBufferSize];
            int bytesRead;
            
            // Read from InputStream and write to file using the direct buffer
            while ((bytesRead = inputStream.read(tempBuffer)) != -1) {
                // Put data into direct buffer
                directBuffer.clear();
                directBuffer.put(tempBuffer, 0, bytesRead);
                directBuffer.flip();
                
                // Write to file channel
                while (directBuffer.hasRemaining()) {
                    fileChannel.write(directBuffer);
                }
                
                totalBytes += bytesRead;
                
                // Log progress periodically for large files
                if (totalBytes % (50 * 1024 * 1024) == 0) {
                    double mbProcessed = totalBytes / (1024.0 * 1024.0);
                    double elapsedSecs = (System.currentTimeMillis() - startTime) / 1000.0;
                    double mbPerSecond = elapsedSecs > 0 ? mbProcessed / elapsedSecs : 0;
                    
                    logger.info("[{}] Progress: {:.2f} MB written ({:.2f} MB/s)", 
                            sessionId, mbProcessed, mbPerSecond);
                }
            }
            
            // Force write to disk
            fileChannel.force(true);
        }
        
        return totalBytes;
    }
    
    /**
     * Verifies that a file starts with the Parquet magic bytes "PAR1"
     * @param file The file to check
     * @param sessionId Session ID for logging
     */
    private static void verifyParquetHeader(File file, String sessionId) {
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] header = new byte[4];
            int headerSize = fis.read(header);
            
            if (headerSize == 4) {
                String headerStr = new String(header);
                logger.info("[{}] File header: '{}'", sessionId, headerStr);
                
                if (!headerStr.equals("PAR1")) {
                    logger.warn("[{}] WARNING: File header does not match Parquet format", sessionId);
                }
            } else {
                logger.warn("[{}] WARNING: Could not read 4 header bytes", sessionId);
            }
        } catch (Exception e) {
            logger.warn("[{}] WARNING: Error checking file header: {}", 
                    sessionId, e.getMessage());
            // Continue despite this error
        }
    }
}
