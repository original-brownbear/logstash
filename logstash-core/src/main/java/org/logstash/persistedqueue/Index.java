package org.logstash.persistedqueue;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public interface Index extends Closeable {

    /**
     * Returns the current read offset for a partition.
     * @param partition Partition Id
     * @return Current read offset for partition
     */
    long watermark(int partition);

    /**
     * Returns the current write offset for a partition.
     * @param partition Partition Id
     * @return Current write offset for partition
     */
    long highWatermark(int partition);

    /**
     * Appends a new offset pair for a partition.
     * @param partition Partition id
     * @param highWatermark High watermark
     * @param watermark Watermark
     */
    void append(int partition, long highWatermark,
        long watermark) throws IOException;

    /**
     * File Backed {@link Index}.
     */
    final class IndexFile implements Index {

        /**
         * Watermark offset array.
         */
        private final long[] watermarks;

        /**
         * Output {@link FileChannel} that {@link StandardOpenOption#DSYNC} appends to the
         * index file.
         */
        private final FileChannel out;

        /**
         * Write Buffer.
         */
        private final ByteBuffer buffer =
            ByteBuffer.allocateDirect(Integer.BYTES + 2 * Long.BYTES);

        IndexFile(final int partitions, final Path dir) throws IOException {
            final Path path = dir.resolve("queue.index");
            watermarks = loadWatermarks(partitions, path);
            out = FileChannel.open(
                path, StandardOpenOption.APPEND,
                StandardOpenOption.DSYNC, StandardOpenOption.CREATE
            );
        }

        @Override
        public synchronized long watermark(final int partition) {
            return this.watermarks[2 * partition];
        }

        @Override
        public synchronized long highWatermark(final int partition) {
            return this.watermarks[2 * partition + 1];
        }

        @Override
        public synchronized void append(final int partition, final long highWatermark,
            final long watermark) throws IOException {
            this.watermarks[2 * partition] = watermark;
            this.watermarks[2 * partition + 1] = highWatermark;
            buffer.position(0);
            buffer.putInt(partition);
            buffer.putLong(watermark);
            buffer.putLong(highWatermark);
            buffer.position(0);
            out.write(buffer);
        }

        @Override
        public void close() throws IOException {
            out.close();
        }

        /**
         * Loads the watermark file from given index path.
         * @param partitions Number of partitions expected to be found in the watermark file
         * @param index Index file path
         * @return Watermark array
         */
        private static long[] loadWatermarks(final int partitions, final Path index) {
            final long[] watermarks = new long[2 * partitions];
            final File file = index.toFile();
            if (file.exists()) {
                try (final DataInputStream stream = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(index.toFile()))
                )
                ) {
                    while (true) {
                        final int part = stream.readInt();
                        watermarks[2 * part] = stream.readLong();
                        watermarks[2 * part + 1] = stream.readLong();
                    }
                } catch (final EOFException ignored) {
                    //End of Index File
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return watermarks;
        }
    }
}
