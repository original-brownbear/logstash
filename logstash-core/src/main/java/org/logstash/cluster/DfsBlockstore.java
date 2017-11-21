package org.logstash.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;

final class DfsBlockstore implements BlockStore {

    private final Path root;

    private final DistributedClusterContext clusterContext;

    DfsBlockstore(final DistributedClusterContext cluster, final String path) {
        clusterContext = cluster;
        root = Paths.get(path);
    }

    @Override
    public void store(final BlockId key, final ByteBuffer buffer) throws IOException {
        final Path parent = root.resolve(key.clusterName).resolve(key.pipeline);
        Files.createDirectories(parent);
        try (
            WritableByteChannel output = FileChannel.open(
                parent.resolve(key.identifier), StandardOpenOption.TRUNCATE_EXISTING
            )
        ) {
            output.write(buffer);
        }
        withLock(
            root.resolve(key.clusterName).resolve(key.pipeline).resolve(key.identifier),
            (Function<Path, Void>) p -> {
                try (
                    WritableByteChannel output = FileChannel.open(
                        p, StandardOpenOption.TRUNCATE_EXISTING
                    )
                ) {
                    output.write(buffer);
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
                return null;
            }
        );
    }

    @Override
    public long load(final BlockId key, final ByteBuffer buffer) throws IOException {
        return withLock(
            root.resolve(key.clusterName).resolve(key.pipeline).resolve(key.identifier),
            file -> {
                final long outstanding;
                if (file.toFile().exists()) {
                    try (SeekableByteChannel input = Files.newByteChannel(file)) {
                        outstanding = input.size() - (long) input.read(buffer);
                    } catch (final IOException ex) {
                        throw new IllegalStateException(ex);
                    }
                } else {
                    outstanding = -1L;
                }
                return outstanding;
            }
        );
    }

    @Override
    public void delete(final BlockId key) throws IOException {
        withLock(
            root.resolve(key.clusterName).resolve(key.pipeline).resolve(key.identifier),
            (Function<Path, Void>) p -> {
                try {
                    Files.delete(p);
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
                return null;
            }
        );
    }

    private static <T> T withLock(final Path file, Function<Path, T> action) throws IOException {
        final Path parent = file.getParent();
        Files.createDirectories(parent);
        try (FileChannel lockChannel =
                 FileChannel.open(file.getParent().resolve(file.getFileName() + ".lock"))) {
            final FileLock lock = lockChannel.lock();
            try {
                return action.apply(file);
            } finally {
                lock.release();
            }
        }
    }
}
