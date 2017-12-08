package org.logstash.cluster.storage.buffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import org.logstash.cluster.utils.AtomixIOException;

/**
 * {@link ByteBuffer} based mapped bytes.
 */
public class MappedBytes extends ByteBufferBytes {

    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel.MapMode mode;
    protected MappedBytes(File file, RandomAccessFile randomAccessFile, MappedByteBuffer buffer, FileChannel.MapMode mode) {
        super(buffer);
        this.file = file;
        this.randomAccessFile = randomAccessFile;
        this.mode = mode;
    }

    /**
     * Allocates a mapped buffer in {@link FileChannel.MapMode#READ_WRITE} mode.
     * <p>
     * Memory will be mapped by opening and expanding the given {@link File} to the desired {@code count} and mapping the
     * file contents into memory via {@link FileChannel#map(FileChannel.MapMode, long, long)}.
     * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
     * @param size The count of the buffer to allocate (in bytes).
     * @return The mapped buffer.
     * @throws NullPointerException If {@code file} is {@code null}
     * @throws IllegalArgumentException If {@code count} is greater than {@link org.logstash.cluster.utils.memory.MappedMemory#MAX_SIZE}
     * @see #allocate(File, FileChannel.MapMode, int)
     */
    public static MappedBytes allocate(File file, int size) {
        return allocate(file, FileChannel.MapMode.READ_WRITE, size);
    }

    /**
     * Allocates a mapped buffer.
     * <p>
     * Memory will be mapped by opening and expanding the given {@link File} to the desired {@code count} and mapping the
     * file contents into memory via {@link FileChannel#map(FileChannel.MapMode, long, long)}.
     * @param file The file to map into memory. If the file doesn't exist it will be automatically created.
     * @param mode The mode with which to map the file.
     * @param size The count of the buffer to allocate (in bytes).
     * @return The mapped buffer.
     * @throws NullPointerException If {@code file} is {@code null}
     * @throws IllegalArgumentException If {@code count} is greater than {@link Integer#MAX_VALUE}
     * @see #allocate(File, int)
     */
    public static MappedBytes allocate(File file, FileChannel.MapMode mode, int size) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, parseMode(mode));
            MappedByteBuffer buffer = randomAccessFile.getChannel().map(mode, 0, size);
            return new MappedBytes(file, randomAccessFile, buffer, mode);
        } catch (IOException e) {
            throw new AtomixIOException(e);
        }
    }

    private static String parseMode(FileChannel.MapMode mode) {
        if (mode == FileChannel.MapMode.READ_ONLY) {
            return "r";
        } else if (mode == FileChannel.MapMode.READ_WRITE) {
            return "rw";
        }
        throw new IllegalArgumentException("unsupported map mode");
    }

    @Override
    protected ByteBuffer newByteBuffer(int size) {
        try {
            return randomAccessFile.getChannel().map(mode, 0, size);
        } catch (IOException e) {
            throw new AtomixIOException(e);
        }
    }

    @Override
    public boolean isDirect() {
        return true;
    }

    @Override
    public Bytes flush() {
        ((MappedByteBuffer) buffer).force();
        return this;
    }

    @Override
    public void close() {
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.close();
    }

    /**
     * Deletes the underlying file.
     */
    public void delete() {
        try {
            close();
            Files.delete(file.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}