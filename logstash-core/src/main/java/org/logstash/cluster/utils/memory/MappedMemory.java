package org.logstash.cluster.utils.memory;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

/**
 * Mapped memory.
 * <p>
 * This is a special memory descriptor that handles management of {@link MappedByteBuffer} based memory. The
 * mapped memory descriptor simply points to the memory address of the underlying byte buffer. When memory is reallocated,
 * the parent {@link MappedMemoryAllocator} is used to create a new {@link MappedByteBuffer}
 * and free the existing buffer.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MappedMemory extends NativeMemory {
    public static final long MAX_SIZE = Integer.MAX_VALUE;
    private final MappedByteBuffer buffer;

    public MappedMemory(MappedByteBuffer buffer, MappedMemoryAllocator allocator) {
        super(((DirectBuffer) buffer).address(), buffer.capacity(), allocator);
        this.buffer = buffer;
    }

    /**
     * Allocates memory mapped to a file on disk.
     * @param file The file to which to map memory.
     * @param size The count of the memory to map.
     * @return The mapped memory.
     * @throws IllegalArgumentException If {@code count} is greater than {@link Integer#MAX_VALUE}
     */
    public static MappedMemory allocate(File file, int size) {
        return new MappedMemoryAllocator(file).allocate(size);
    }

    /**
     * Allocates memory mapped to a file on disk.
     * @param file The file to which to map memory.
     * @param mode The mode with which to map memory.
     * @param size The count of the memory to map.
     * @return The mapped memory.
     * @throws IllegalArgumentException If {@code count} is greater than {@link Integer#MAX_VALUE}
     */
    public static MappedMemory allocate(File file, FileChannel.MapMode mode, int size) {
        if (size > MAX_SIZE)
            throw new IllegalArgumentException("size cannot be greater than " + MAX_SIZE);
        return new MappedMemoryAllocator(file, mode).allocate(size);
    }

    /**
     * Flushes the mapped buffer to disk.
     */
    public void flush() {
        buffer.force();
    }

    public void close() {
        free();
        ((MappedMemoryAllocator) allocator).close();
    }

    @Override
    public void free() {
        Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        if (cleaner != null)
            cleaner.clean();
        ((MappedMemoryAllocator) allocator).release();
    }

}
