package org.logstash.cluster.utils.memory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mapped memory allocator.
 * <p>
 * The mapped memory allocator provides direct memory access to memory mapped from a file on disk. The mapped allocator
 * supports allocating memory in any {@link FileChannel.MapMode}. Once the file is mapped and the
 * memory has been allocated, the mapped allocator provides the memory address of the underlying
 * {@link java.nio.MappedByteBuffer} for access via {@link sun.misc.Unsafe}.
 */
public class MappedMemoryAllocator implements MemoryAllocator<MappedMemory> {
    public static final FileChannel.MapMode DEFAULT_MAP_MODE = FileChannel.MapMode.READ_WRITE;

    private final AtomicInteger referenceCount = new AtomicInteger();
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final FileChannel.MapMode mode;
    private final long offset;

    public MappedMemoryAllocator(File file) {
        this(file, DEFAULT_MAP_MODE, 0);
    }

    public MappedMemoryAllocator(File file, FileChannel.MapMode mode, long offset) {
        this(createFile(file, mode), mode, offset);
    }

    public MappedMemoryAllocator(RandomAccessFile file, FileChannel.MapMode mode, long offset) {
        if (file == null)
            throw new NullPointerException("file cannot be null");
        if (mode == null)
            throw new NullPointerException("mode cannot be null");
        if (offset < 0)
            throw new IllegalArgumentException("offset cannot be negative");
        this.file = file;
        this.channel = this.file.getChannel();
        this.mode = mode;
        this.offset = offset;
    }

    private static RandomAccessFile createFile(File file, FileChannel.MapMode mode) {
        if (file == null)
            throw new NullPointerException("file cannot be null");
        if (mode == null)
            mode = DEFAULT_MAP_MODE;
        try {
            return new RandomAccessFile(file, parseMode(mode));
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    public MappedMemoryAllocator(File file, FileChannel.MapMode mode) {
        this(file, mode, 0);
    }

    /**
     * Releases a reference from the allocator.
     */
    void release() {
        if (referenceCount.decrementAndGet() == 0) {
            close();
        }
    }    @Override
    public MappedMemory allocate(int size) {
        try {
            if (file.length() < size)
                file.setLength(size);
            referenceCount.incrementAndGet();
            return new MappedMemory(channel.map(mode, offset, size), this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }    @Override
    public MappedMemory reallocate(MappedMemory memory, int size) {
        MappedMemory newMemory = allocate(size);
        memory.free();
        return newMemory;
    }





}
