package org.logstash.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

final class DfsBlockstore implements BlockStore {

    private final Path root;

    DfsBlockstore(final String path) {
        root = Paths.get(path);
    }

    @Override
    public void store(final BlockId key, final ByteBuffer buffer) throws IOException {
        withLock(
            root.resolve(key.clusterName).resolve(key.group).resolve(key.identifier),
            (Function<Path, Void>) p -> {
                try (
                    WritableByteChannel output = FileChannel.open(
                        p, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW
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
    public Void load(final BlockId key, final ByteBuffer buffer) throws IOException {
        return withLock(
            root.resolve(key.clusterName).resolve(key.group).resolve(key.identifier),
            file -> {
                if (file.toFile().exists()) {
                    try (SeekableByteChannel input = Files.newByteChannel(file)) {
                        input.read(buffer);
                    } catch (final IOException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
                return null;
            }
        );
    }

    @Override
    public void delete(final BlockId key) throws IOException {
        withLock(
            root.resolve(key.clusterName).resolve(key.identifier),
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

    @Override
    public Iterator<BlockId> group(final BlockId key) throws IOException {
        final Collection<BlockId> blockIds = new ArrayList<>();
        try (DirectoryStream<Path> dir =
                 Files.newDirectoryStream(root.resolve(key.clusterName).resolve(key.group))) {
            for (final Path file : dir) {
                if (!file.getFileName().toString().endsWith(".lock")) {
                    blockIds.add(
                        new BlockId(key.clusterName, key.group, file.getFileName().toString())
                    );
                }
            }
        }
        return blockIds.iterator();
    }

    private static <T> T withLock(final Path file, final Function<Path, T> action)
        throws IOException {
        final Path parent = file.getParent();
        Files.createDirectories(parent);
        try (
            FileChannel lockChannel = FileChannel.open(
                parent.resolve(file.getFileName() + ".lock"), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
            )
        ) {
            final FileLock lock = lockChannel.lock();
            try {
                return action.apply(file);
            } finally {
                lock.release();
            }
        }
    }

    @Override
    public void close() {
        //noop
    }
}
