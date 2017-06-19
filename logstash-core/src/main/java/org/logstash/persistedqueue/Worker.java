package org.logstash.persistedqueue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.logstash.Event;

public interface Worker extends Runnable, Closeable {

    /**
     * Returns {@code true} iff all data in this worker's backing datafile and associated
     * buffers has been consumed.
     * @return {@code true} iff all data handled by this worker has been flushed to a
     * consumer
     */
    boolean flushed();

    /**
     * Signal this worker to stop processing {@link Event} and flush all internal buffers.
     */
    void shutdown();

    /**
     * <p>Background worker passing events from {@link PersistedQueue.Local#writeBuffer} to
     * {@link PersistedQueue.Local#readBuffer} while simultaneously persisting them to the
     * file system.</p>
     * <p>This worker tries to actively (in a blocking manner) promote persisted data to 
     * deserialized in memory storage as capacity becomes available.</p>
     */
    final class LogWorker implements Worker {
    
        /**
         * Offset Index Database.
         */
        private final Index index;
    
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
         * {@link FileChannel} used in conjunction with
         * {@link LogWorker#obuf} to physically write to the file system.
         */
        private final FileChannel out;
    
        /**
         * {@link FileChannel} used to read persisted data from the filesystem.
         */
        private final FileChannel in;
    
        /**
         * Output buffer used to buffer writes by {@link LogWorker#out}.
         */
        private final ByteBuffer obuf = ByteBuffer.allocateDirect(BYTE_BUFFER_SIZE);
    
        /**
         * Input buffer used to buffer reads by {@link LogWorker#in}.
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
         * {@link LogWorker#in} to.
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
         * {@link LogWorker#outBuffer} is depleted.</p>
         * Note that {@link LogWorker#obuf} must be flushed in order to
         * make this number correspond to a physical offset on
         * {@link LogWorker#in}.
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
         * {@link LogWorker#readBuffer}.
         */
        private long flushed;
    
        /**
         * Ctor.
         * @param index {@link Index.IndexFile} storing offsets.
         * @param file Backing data {@link Path}
         * @param readBuffer Same instance as {@link PersistedQueue.Local#readBuffer}
         * @param writeBuffer Same instance as {@link PersistedQueue.Local#writeBuffer}
         * @param ack Maximum number of in-flight events, that are either stored serialized
         * in {@link LogWorker#obuf} or already written to
         * {@link LogWorker#out}, but not yet `fsync`ed to the file system
         * @throws IOException On failure to open backing data file for either reads or writes
         */
        LogWorker(final Index index, final Path file,
            final int partition, final BlockingQueue<Event> readBuffer,
            final BlockingQueue<Event> writeBuffer, final int ack) throws IOException {
            this.index = index;
            this.partition = partition;
            this.out = 
                FileChannel.open(file, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            this.in = FileChannel.open(file, StandardOpenOption.READ);
            this.readBuffer = readBuffer;
            this.writeBuffer = writeBuffer;
            count = 0L;
            flushed = 0L;
            highWatermarkPos = this.index.highWatermark(partition);
            watermarkPos = this.index.watermark(partition);
            // Use half the ack interval for serialized buffering, except for the case of
            // ack == 1 which has serialized buffering only
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
        public boolean flushed() {
            return highWatermarkPos == watermarkPos && obuf.position() == 0 &&
                outBuffer.isEmpty();
        }
    
        @Override
        public void close() throws IOException {
            this.awaitShutdown();
            this.in.close();
            this.out.close();
            this.index.append(partition, watermarkPos, highWatermarkPos);
        }
    
        /**
         * Wait for this worker to stop and flush all internal buffers.
         */
        private void awaitShutdown() {
            try {
                this.shutdown.await();
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(ex);
            }
        }
    
        /**
         * Sets the watermark for number of bytes processed to the bound of all {@link Event}
         * data that has been enqueued in either
         * {@link LogWorker#outBuffer} or
         * {@link LogWorker#readBuffer}.
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
            if (count == flushed - 1L && this.readBuffer.offer(event)
                || fullyRead && outBuffer.offer(event)) {
                this.completeWatermark();
            }
        }
    
        /**
         * Flush {@link LogWorker#obuf} to the filesystem if another write
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
         * <p>Flushes {@link LogWorker#obuf} to the filesystem.</p>
         * Note that the method triggers `fsync` and therefore guarantees physical persistence
         * within the limits of the backing file system.
         * @throws IOException On failure to flush buffer to filesystem
         */
        private void flush() throws IOException {
            obuf.flip();
            highWatermarkPos += (long) out.write(obuf);
            out.force(true);
            index.append(partition, watermarkPos, highWatermarkPos);
            obuf.clear();
        }
    
        /**
         * Tries to advance one {@link Event} from
         * {@link LogWorker#outBuffer} to
         * {@link LogWorker#readBuffer}.
         * @return {@code true} iff an {@link Event} was promoted from the un-contended
         * {@link LogWorker#outBuffer} to the contended
         * {@link LogWorker#readBuffer}
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
         * Promotes {@link Event}s that are only buffered in serialized form in the file system
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
            final int size = outBuffer.size();
            while (i < size && this.advanceBuffers()) {
                ++i;
            }
            int remaining = outBuffer.remainingCapacity();
            final int before = remaining;
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
            }
            return before > remaining;
        }
    }
}
