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
 * Utility class for optimized I/O operations on resource-constrained university systems
 * Uses smaller buffers and more conservative memory handling to prevent corruption
 */
public class UniversityBufferIO {
    private static final Logger logger = LoggerFactory.getLogger(UniversityBufferIO.class);
    
    // Smaller buffer sizes for resource-constrained environments
    private static final int DEFAULT_DIRECT_BUFFER_SIZE = 1 * 1024 * 1024; // 1 MB (reduced)
    private static final int DEFAULT_TEMP_BUFFER_SIZE = 32 * 1024;         // 32 KB (reduced)
    
    // Custom upload directory - helps avoid /tmp space limitations
    private static String uploadDir = null;
    
    /**
     * Set custom upload directory 
     * @param dir Custom directory for uploads (must be writable)
     */
    public static void setUploadDirectory(String dir) {
        File directory = new File(dir);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        
        if (directory.canWrite()) {
            uploadDir = dir;
            logger.info("Upload directory set to: {}", uploadDir);
        } else {
            logger.warn("Cannot write to directory: {} - will use default temp", dir);
        }
    }
    
    /**
     * Saves an InputStream to a temporary file using smaller buffers 
     * for resource-constrained environments
     * @param inputStream The input stream to save
     * @param prefix File name prefix (optional)
     * @param suffix File extension (optional)
     * @return The created temporary file
     * @throws IOException If an I/O error occurs
     */
    public static File saveToTempFile(InputStream inputStream, String prefix, String suffix) throws IOException {
        String sessionId = UUID.randomUUID().toString().substring(0, 8); // Shorter id
        String filePrefix = prefix != null ? prefix : "uniupload_";
        String fileSuffix = suffix != null ? suffix : ".tmp";
        
        logger.info("[{}] Starting university-optimized file upload", sessionId);
        
        // Validate input
        if (inputStream == null) {
            throw new IOException("InputStream is null");
        }
        
        // Create temp file (in custom directory if set)
        File tempFile;
        if (uploadDir != null) {
            tempFile = new File(uploadDir, filePrefix + sessionId + fileSuffix);
        } else {
            // Use user's home directory if available, falling back to /tmp
            String homeDir = System.getProperty("user.home");
            if (homeDir != null && new File(homeDir).canWrite()) {
                File userTempDir = new File(homeDir, "memorydb_temp");
                userTempDir.mkdirs();
                
                if (userTempDir.canWrite()) {
                    tempFile = new File(userTempDir, filePrefix + sessionId + fileSuffix);
                } else {
                    tempFile = File.createTempFile(filePrefix, fileSuffix);
                }
            } else {
                tempFile = File.createTempFile(filePrefix, fileSuffix);
            }
        }
        
        logger.info("[{}] Temporary file created: {}", sessionId, tempFile.getAbsolutePath());
        
        // Ensure file permissions
        tempFile.setReadable(true, false);
        tempFile.setWritable(true, false);
        
        // Transfer with conservative buffer
        long totalBytes = 0;
        long startTime = System.currentTimeMillis();
        
        try {
            totalBytes = transferToFileWithSmallChunks(
                inputStream, 
                tempFile, 
                DEFAULT_DIRECT_BUFFER_SIZE,
                DEFAULT_TEMP_BUFFER_SIZE,
                sessionId
            );
            
            double mbTransferred = totalBytes / (1024.0 * 1024.0);
            double elapsedSecs = (System.currentTimeMillis() - startTime) / 1000.0;
            double mbPerSec = elapsedSecs > 0 ? mbTransferred / elapsedSecs : 0;
            
            logger.info("[{}] University file transfer complete: {:.2f} MB in {:.2f} sec ({:.2f} MB/sec)",
                    sessionId, mbTransferred, elapsedSecs, mbPerSec);
            
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
     * Transfer data using smaller chunks and more frequent syncs
     * @param inputStream Source input stream
     * @param outputFile Destination file
     * @param directBufferSize Size of the direct buffer (in bytes)
     * @param tempBufferSize Size of the temporary buffer for reading
     * @param sessionId Session ID for logging
     * @return Total bytes transferred
     * @throws IOException If an I/O error occurs
     */
    private static long transferToFileWithSmallChunks(
            InputStream inputStream, 
            File outputFile, 
            int directBufferSize,
            int tempBufferSize,
            String sessionId) throws IOException {
        
        long totalBytesRead = 0;
        int chunks = 0;
        
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             FileChannel channel = fos.getChannel()) {
            
            ByteBuffer buffer = ByteBuffer.allocateDirect(directBufferSize);
            byte[] tempBuffer = new byte[tempBufferSize];
            
            int bytesRead;
            while ((bytesRead = inputStream.read(tempBuffer, 0, tempBufferSize)) != -1) {
                buffer.clear();
                buffer.put(tempBuffer, 0, bytesRead);
                buffer.flip();
                
                // Write chunk to file
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                
                totalBytesRead += bytesRead;
                chunks++;
                
                // More frequent synchronization for safety
                if (chunks % 20 == 0) {
                    channel.force(false); // Sync data but not metadata
                    logger.debug("[{}] Synced after chunk {}, total bytes: {}", 
                            sessionId, chunks, totalBytesRead);
                }
            }
            
            // Final force to ensure data is written
            channel.force(true);
            
            logger.info("[{}] File transfer complete: {} bytes in {} chunks", 
                    sessionId, totalBytesRead, chunks);
            
            return totalBytesRead;
        }
    }
    
    /**
     * Verify a file exists and has minimum required size
     * @param file File to verify
     * @return true if file exists and has content
     */
    public static boolean verifyFile(File file) {
        if (!file.exists()) {
            logger.error("File does not exist: {}", file.getAbsolutePath());
            return false;
        }
        
        if (file.length() < 100) {  // Arbitrary small size check
            logger.error("File is too small (possibly corrupted): {} bytes", file.length());
            return false;
        }
        
        return true;
    }
}
