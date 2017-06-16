package org.logstash.persistedqueue;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.logstash.Event;

/**
 * <p>Queue of {@link Event} expected to be backed by some form of physical storage.</p>
 * Implementations are assumed to be threadsafe.
 */
public interface PersistedQueue extends Closeable {

    /**
     * <p>Enqueues an {@link Event} in a blocking fashion.</p>
     * Implementations should behave analogous to {@link ArrayBlockingQueue#put(Object)}.
     * @param event Event to enqueue
     * @throws InterruptedException On interrupt during enqueue
     */
    void enqueue(Event event) throws InterruptedException;

    /**
     * <p>Dequeues an {@link Event} in a blocking fashion.</p>
     * Implementations should behave analogous to {@link ArrayBlockingQueue#take()}.
     * @return {@link Event}.
     * @throws InterruptedException On interrupt during dequeue
     */
    Event dequeue() throws InterruptedException;

    /**
     * <p>Dequeues an {@link Event} in a blocking fashion, subject to a timeout.</p>
     * Implementations should behave analogous to {@link ArrayBlockingQueue#poll(long, TimeUnit)}.
     * @return {@link Event}.
     * @throws InterruptedException On interrupt during dequeue
     */
    Event poll(long timeout, TimeUnit unit) throws InterruptedException;

    boolean empty();
    
    /**
     * <p>Implementation of a local-disk-backed {@link Event} queue.</p>
     * This queue guarantees the number of {@link Event} that are in-flight and not yet physically
     * written to disk is limited by a configurable upper bound.
     */
    final class Local implements PersistedQueue {

        /**
         * <p>Concurrency Level.</p>
         * A level of 1 results in complete ordering, higher levels do not come with a guaranteed
         * ordering between inputs and outputs.
         */
        private static final int CONCURRENT = 1;

        /**
         * {@link ExecutorService} managing worker threads.
         */
        private final ExecutorService exec = Executors.newFixedThreadPool(CONCURRENT);

        /**
         * Workers moving data from {@link PersistedQueue.Local#writeBuffer} and
         * {@link PersistedQueue.Local#readBuffer} while simultaneously persisting it to the file
         * system.
         */
        private final PersistedQueue.Local.Worker[] workers =
            new PersistedQueue.Local.Worker[CONCURRENT];

        private final PersistedQueue.Local.Index indexFile;

        /**
         * Buffer for incoming {@link Event} that have yet to be written to the file system.
         * Chosen to be half the size of the upper bound of allowed in-flight {@link Event}, with
         * the other half of the allow in-flight event count being used for worker I/O buffering
         * and timing `fsync` calls.
         */
        private final BlockingQueue<Event> writeBuffer;

        /**
         * <p>Buffer for already persisted {@link Event} reader for processing and available to queue
         * consumers via {@link PersistedQueue.Local#dequeue()}.</p>
         * Chosen to be of a fixed size of {@code 1024}.
         */
        private final BlockingQueue<Event> readBuffer;

        /**
         * Ctor.
         * @param ack Maximum number of in flight {@link Event}
         * @param directory Directory to store backing data in
         */
        public Local(final int ack, final String directory) {
            this.writeBuffer = writebuf(ack / 2);
            this.readBuffer = new ArrayBlockingQueue<>(1024);
            try {
                this.indexFile =
                    new PersistedQueue.Local.IndexFile(CONCURRENT, Paths.get(directory));
                for (int i = 0; i < CONCURRENT; ++i) {
                    this.workers[i] = new PersistedQueue.Local.LogWorker(
                        this.indexFile,
                        Paths.get(directory, String.format("%d.log", i)).toFile(), i,
                        readBuffer, writeBuffer, ack
                    );
                    this.exec.execute(workers[i]);
                }
            } catch (final IOException ex) {
                throw new IllegalStateException(ex);
            }
        }

        @Override
        public void enqueue(final Event event) throws InterruptedException {
            writeBuffer.put(event);
        }

        @Override
        public Event dequeue() throws InterruptedException {
            return readBuffer.take();
        }

        @Override
        public Event poll(final long timeout, final TimeUnit unit) throws InterruptedException {
            return readBuffer.poll(timeout, unit);
        }

        @Override
        public boolean empty() {
            boolean res = true;
            for (int i = 0; i < CONCURRENT; ++i) {
                res = res && this.workers[i].flushed();
                if (!res) {
                    break;
                }
            }
            return res && readBuffer.isEmpty() && writeBuffer.isEmpty();
        }
        
        @Override
        public void close() throws IOException {
            for (int i = 0; i < CONCURRENT; ++i) {
                this.workers[i].shutdown();
                this.workers[i].close();
            }
            this.indexFile.close();
            exec.shutdown();
        }
        
        private static BlockingQueue<Event> writebuf(final int ack) {
            final BlockingQueue<Event> res;
            if (ack > 1) {
                res = new ArrayBlockingQueue<>(ack);
            } else {
                res = new SynchronousQueue<>();
            }
            return res;
        }

        private interface Worker extends Runnable, Closeable {

            boolean flushed();
            
            /**
             * Wait for this worker to stop and flush all internal buffers.
             */
            void awaitShutdown();

            /**
             * Signal this worker to stop processing {@link Event} and flush all internal buffers.
             */
            void shutdown();
        }

        /**
         * Background worker passing events from {@link PersistedQueue.Local#writeBuffer} to
         * {@link PersistedQueue.Local#readBuffer} while simultaneously persisting them to the
         * file system.
         */
        private static final class LogWorker implements PersistedQueue.Local.Worker {

            private final PersistedQueue.Local.Index index;

            /**
             * Size (in number of {@link Event}) of the un-contended output queue-buffer available
             * to this worker.
             */
            private static final int OUT_BUFFER_SIZE = 100;

            /**
             * Size (in byte) of the internal I/O buffers. Gives the upper bound for the size of
             * a serialized {@link Event} representation.
             */
            private static final int BYTE_BUFFER_SIZE = 1 << 16;

            /**
             * Maximum number of in-flight {@link Event} that have yet to be persisted to disk.
             */
            private final int ack;

            /**
             * {@link FileOutputStream} of the backing data file. Referenced here to be able to
             * safely manage closing of the output file descriptor.
             */
            private final FileOutputStream file;

            /**
             * {@link FileDescriptor} of the backing append file used to trigger `fsync`.
             */
            private final FileDescriptor fd;

            /**
             * {@link FileChannel} used in conjunction with
             * {@link PersistedQueue.Local.LogWorker#obuf} to physically write to the file system.
             */
            private final FileChannel out;

            /**
             * {@link FileChannel} used to read persisted data from the filesystem.
             */
            private final FileChannel in;

            /**
             * Output buffer used to buffer writes by {@link PersistedQueue.Local.LogWorker#out}.
             */
            private final ByteBuffer obuf = ByteBuffer.allocateDirect(BYTE_BUFFER_SIZE);

            /**
             * Input buffer used to buffer reads by {@link PersistedQueue.Local.LogWorker#in}.
             */
            private final ByteBuffer ibuf = ByteBuffer.allocateDirect(BYTE_BUFFER_SIZE);

            /**
             * Incoming {@link Event} buffer shared across all workers on a
             * {@link PersistedQueue.Local}.
             */
            private final BlockingQueue<Event> writeBuffer;

            /**
             * Outgoing {@link Event} buffer shared across all workers on a
             * {@link PersistedQueue.Local}.
             */
            private final BlockingQueue<Event> readBuffer;

            /**
             * {@link CountDownLatch} indicating that this worker has no more in flight events or
             * buffered data and can be safely closed.
             */
            private final CountDownLatch shutdown = new CountDownLatch(1);

            /**
             * {@link AtomicBoolean} indicating that this worker is active.
             */
            private final AtomicBoolean running = new AtomicBoolean(true);

            /**
             * Un-contended queue to buffer {@link Event} deserialized from reads on
             * {@link PersistedQueue.Local.LogWorker#in} to.
             */
            private final ArrayBlockingQueue<Event> outBuffer =
                new ArrayBlockingQueue<>(OUT_BUFFER_SIZE);

            /**
             * Deserialization buffer used to pass data to {@link Event#deserialize(byte[])}.
             */
            private final byte[] readByteBuffer = new byte[BYTE_BUFFER_SIZE];

            /**
             * Partition of this worker.
             */
            private final int partition;

            /**
             * <p>Offset on the backing file to read the next {@link Event} from in case
             * {@link PersistedQueue.Local.LogWorker#outBuffer} is depleted.</p>
             * Note that {@link PersistedQueue.Local.LogWorker#obuf} must be flushed in order to
             * make this number correspond to a physical offset on
             * {@link PersistedQueue.Local.LogWorker#in}.
             */
            private long watermarkPos;

            /**
             * Largest valid offset on the backing data file.
             */
            private long highWatermarkPos;

            /**
             * Number of {@link Event} written to disk.
             */
            private long count;

            /**
             * Number of {@link Event} successfully passed to
             * {@link PersistedQueue.Local.LogWorker#readBuffer}.
             */
            private long flushed;

            /**
             * Ctor.
             * @param index {@link PersistedQueue.Local.IndexFile} storing offsets.
             * @param file Backing data {@link File}
             * @param readBuffer Same instance as {@link PersistedQueue.Local#readBuffer}
             * @param writeBuffer Same instance as {@link PersistedQueue.Local#writeBuffer}
             * @param ack Maximum number of in-flight events, that are either stored serialized
             * in {@link PersistedQueue.Local.LogWorker#obuf} or already written to
             * {@link PersistedQueue.Local.LogWorker#out}, but not yet `fsync`ed to the file system
             * @throws IOException On failure to open backing data file for either reads or writes
             */
            LogWorker(final PersistedQueue.Local.Index index, final File file,
                final int partition, final BlockingQueue<Event> readBuffer,
                final BlockingQueue<Event> writeBuffer, final int ack) throws IOException {
                this.index = index;
                this.partition = partition;
                this.file = new FileOutputStream(file, true);
                this.fd = this.file.getFD();
                this.out = this.file.getChannel();
                this.in = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                this.readBuffer = readBuffer;
                this.writeBuffer = writeBuffer;
                count = 0;
                flushed = 0;
                highWatermarkPos = this.index.highWatermark(partition);
                watermarkPos = this.index.watermark(partition);
                this.ack = Math.max(1, ack / 2);
            }

            @Override
            public void run() {
                while (running.get()) {
                    try {
                        final Event event = this.writeBuffer.poll(10L, TimeUnit.MILLISECONDS);
                        if (event != null) {
                            write(event);
                        }
                        if (count % (long) ack == 0L && obuf.position() > 0) {
                            flush();
                        }
                        int j = 0;
                        while (j < 5 && advanceFile()) {
                            ++j;
                        }
                    } catch (final InterruptedException | IOException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
                try {
                    flush();
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
                this.shutdown.countDown();
            }

            @Override
            public void shutdown() {
                this.running.set(false);
            }

            @Override
            public void awaitShutdown() {
                try {
                    this.shutdown.await();
                } catch (final InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
            }
            
            @Override
            public boolean flushed() {
                return highWatermarkPos == watermarkPos && obuf.position() == 0 &&
                    outBuffer.isEmpty();
            }

            @Override
            public void close() throws IOException {
                this.awaitShutdown();
                this.in.close();
                this.file.close();
                this.index.append(partition, watermarkPos, highWatermarkPos);
            }
  
            /**
             * Sets the watermark for number of bytes processed to the bound of all {@link Event}
             * data that has been enqueued in either
             * {@link PersistedQueue.Local.LogWorker#outBuffer} or
             * {@link PersistedQueue.Local.LogWorker#readBuffer}.
             */
            private void completeWatermark() {
                watermarkPos = highWatermarkPos + (long) obuf.position();
            }

            /**
             * Serialize and write a {@link Event} to the backing file.
             * @param event Event to persist
             * @throws IOException On failure to serialize or write event to underlying storage
             */
            private void write(final Event event) throws IOException {
                final boolean fullyRead =
                    highWatermarkPos + (long) obuf.position() == this.watermarkPos;
                ++count;
                final byte[] data = event.serialize();
                maybeFlush(data.length + Integer.BYTES);
                obuf.putInt(data.length);
                obuf.put(data);
                if (count == flushed - 1L && this.readBuffer.offer(event)) {
                    this.completeWatermark();
                } else {
                    if (fullyRead && outBuffer.offer(event)) {
                        this.completeWatermark();
                    }
                }
            }

            /**
             * Flush {@link PersistedQueue.Local.LogWorker#obuf} to the filesystem if another write
             * of given size could not be buffered to it.
             * @param size Size of the next write
             * @throws IOException On failure to flush buffer to filesystem
             */
            private void maybeFlush(final int size) throws IOException {
                final int pos = obuf.position();
                if (pos > 0 && BYTE_BUFFER_SIZE - pos < size) {
                    flush();
                }
            }

            /**
             * <p>Flushes {@link PersistedQueue.Local.LogWorker#obuf} to the filesystem.</p>
             * Note that the method triggers `fsync` and therefore guarantees physical persistence
             * within the limits of the backing file system.
             * @throws IOException On failure to flush buffer to filesystem
             */
            private void flush() throws IOException {
                obuf.flip();
                highWatermarkPos += (long) out.write(obuf);
                fd.sync();
                index.append(partition, watermarkPos, highWatermarkPos);
                obuf.clear();
            }

            /**
             * Tries to advance one {@link Event} from
             * {@link PersistedQueue.Local.LogWorker#outBuffer} to
             * {@link PersistedQueue.Local.LogWorker#readBuffer}.
             * @return {@code true} iff an {@link Event} was promoted from the un-contended
             * {@link PersistedQueue.Local.LogWorker#outBuffer} to the contended
             * {@link PersistedQueue.Local.LogWorker#readBuffer}
             */
            private boolean advanceBuffers() {
                final boolean result;
                final Event e = outBuffer.peek();
                if (e != null && readBuffer.offer(e)) {
                    outBuffer.poll();
                    flushed++;
                    result = true;
                } else {
                    result = false;
                }
                return result;
            }

            /**
             * Buffers {@link Event} that are only buffered in serialized form in the file system
             * to deserialized buffers.
             * @return {@code true} iff at least one {@link Event} was deserialized and buffered
             * @throws IOException On failure to read from underlying storage
             */
            private boolean advanceFile() throws IOException {
                if (flushed + (long) outBuffer.size() < count &&
                    this.watermarkPos == highWatermarkPos) {
                    this.flush();
                }
                int i = 0;
                while (i < OUT_BUFFER_SIZE && this.advanceBuffers()) {
                    ++i;
                }
                final boolean result;
                int remaining = outBuffer.remainingCapacity();
                if (remaining > 0 && this.watermarkPos < highWatermarkPos) {
                    this.in.position(watermarkPos);
                    ibuf.clear();
                    this.in.read(ibuf);
                    ibuf.flip();
                    while (ibuf.remaining() >= Integer.BYTES && remaining > 0) {
                        final int len = ibuf.getInt();
                        ibuf.get(readByteBuffer, 0, len);
                        outBuffer.add(Event.deserialize(readByteBuffer));
                        this.watermarkPos += (long) (len + Integer.BYTES);
                        --remaining;
                    }
                    result = true;
                } else {
                    result = false;
                }
                return result;
            }
        }

        private interface Index extends Closeable {

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
        }

        private static final class IndexFile implements PersistedQueue.Local.Index {

            private final long[] watermarks;

            private final FileChannel out;

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
}
