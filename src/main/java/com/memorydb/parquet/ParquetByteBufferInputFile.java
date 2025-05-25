package com.memorydb.parquet;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An implementation of InputFile that reads from a ByteBuffer directly.
 * This avoids the need to create temporary files for Parquet processing.
 */
public class ParquetByteBufferInputFile implements InputFile {
    
    private final ByteBuffer data;
    private final long length;

    public ParquetByteBufferInputFile(ByteBuffer data) {
        // Create a duplicate to avoid affecting the original buffer's position/limit
        this.data = data.duplicate();
        this.length = data.remaining();
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        // Create a new stream with a duplicated buffer (to allow independent positioning)
        return new ByteBufferSeekableInputStream(data.duplicate());
    }

    /**
     * Implementation of SeekableInputStream that reads from a ByteBuffer.
     */
    private static class ByteBufferSeekableInputStream extends SeekableInputStream {
        private final ByteBuffer buffer;

        ByteBufferSeekableInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
            // Ensure the position is at the beginning
            this.buffer.position(0);
        }

        @Override
        public long getPos() {
            return buffer.position();
        }

        @Override
        public void seek(long newPos) {
            buffer.position((int) newPos);
        }

        @Override
        public int read() {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            // ByteBuffer.get() returns a byte which is signed in Java, but we need to return
            // an unsigned byte value as an int (0-255), as per the InputStream contract
            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            
            int available = buffer.remaining();
            int toRead = Math.min(available, len);
            
            buffer.get(b, off, toRead);
            return toRead;
        }

        @Override
        public void readFully(byte[] bytes) {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) {
            if (len > buffer.remaining()) {
                throw new RuntimeException("Not enough bytes available to read");
            }
            buffer.get(bytes, start, len);
        }

        @Override
        public int read(ByteBuffer buf) {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            
            int toRead = Math.min(buffer.remaining(), buf.remaining());
            
            // Create a temporary slice of the input buffer
            ByteBuffer slice = buffer.duplicate();
            slice.limit(slice.position() + toRead);
            
            // Put this slice into the destination buffer
            buf.put(slice);
            
            // Advance the position of our internal buffer
            buffer.position(buffer.position() + toRead);
            
            return toRead;
        }
        
        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            if (buf.remaining() > buffer.remaining()) {
                throw new IOException("Not enough bytes available to read fully");
            }
            
            int originalLimit = buf.limit();
            int originalPos = buf.position();
            int length = buf.remaining();
            
            int bytesRead = read(buf);
            if (bytesRead < length) {
                buf.limit(originalLimit);
                buf.position(originalPos + bytesRead);
                throw new IOException("Could not read requested bytes");
            }
        }

        @Override
        public void close() {
            // Nothing to close for a ByteBuffer
        }
    }
}
