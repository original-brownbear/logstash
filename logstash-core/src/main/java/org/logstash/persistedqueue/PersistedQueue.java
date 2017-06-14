package org.logstash.persistedqueue;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.logstash.Event;
import org.logstash.ackedqueue.Queueable;
import org.logstash.ext.JrubyEventExtLibrary;

public interface PersistedQueue extends Closeable {

    /**
     * Enqueues an {@link Event} in a blocking fashion.<br />
     * Implementations should behave analogous to {@link ArrayBlockingQueue#put(Object)}.
     * @param event Event to enqueue
     * @throws InterruptedException On interrupt during enqueue
     */
    void enqueue(Event event) throws InterruptedException;

    /**
     * Dequeues an {@link Event} in a blocking fashion.<br />
     * Implementations should behave analogous to {@link ArrayBlockingQueue#take()}.
     * @return {@link Event}.
     * @throws InterruptedException On interrupt during dequeue
     */
    Event dequeue() throws InterruptedException;

    /**
     * Dequeues an {@link Event} in a blocking fashion, subject to a timeout.<br />
     * Implementations should behave analogous to {@link ArrayBlockingQueue#poll(long, TimeUnit)}.
     * @return {@link Event}.
     * @throws InterruptedException On interrupt during dequeue
     */
    Event poll(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Implementation of a local-disk-backed {@link Event} queue.<br />
     * This queue guarantees the number of {@link Event} that are in-flight and not yet physically
     * written to disk is limited by a configurable upper bound.
     */
    final class Local implements PersistedQueue {

        /**
         * Concurrency Level. <br />
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
        private final PersistedQueue.Local.LogWorker[] workers =
            new PersistedQueue.Local.LogWorker[CONCURRENT];

        /**
         * Buffer for incoming {@link Event} that have yet to be written to the file system.
         * Chosen to be half the size of the upper bound of allowed in-flight {@link Event}, with
         * the other half of the allow in-flight event count being used for worker I/O buffering
         * and timing `fsync` calls.
         */
        private final ArrayBlockingQueue<Event> writeBuffer;

        /**
         * Buffer for already persisted {@link Event} reader for processing and available to queue
         * consumers via {@link PersistedQueue.Local#dequeue()}.<br />
         * Chosen to be of a fixed size of {@code 1024}.
         */
        private final ArrayBlockingQueue<Event> readBuffer;

        /**
         * Ctor.
         * @param ack Maximum number of in flight {@link Event}
         * @param directory Directory to store backing data in
         */
        public Local(final int ack, final String directory) {
            this.writeBuffer = new ArrayBlockingQueue<>(ack / 2);
            this.readBuffer = new ArrayBlockingQueue<>(1024);
            for (int i = 0; i < CONCURRENT; ++i) {
                try {
                    this.workers[i] = new PersistedQueue.Local.LogWorker(
                        Paths.get(directory, String.format("%d.log", i)).toFile(),
                        readBuffer, writeBuffer, ack / (2 * CONCURRENT)
                    );
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
                this.exec.execute(workers[i]);
            }
        }

        /**
         * Wrapper {@link PersistedQueue.Local#enqueue(Event)} for calls from Ruby code.
         * @param event {@link Event} to enqueue
         * @throws InterruptedException On interrupt during enqueuing
         */
        public void enqueue(final JrubyEventExtLibrary.RubyEvent event)
            throws InterruptedException {
            this.enqueue(event.getEvent());
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
        public void close() throws IOException {
            for (int i = 0; i < CONCURRENT; ++i) {
                this.workers[i].shutdown();
                this.workers[i].close();
            }
            exec.shutdown();
        }

        /**
         * Background worker passing events from {@link PersistedQueue.Local#writeBuffer} to
         * {@link PersistedQueue.Local#readBuffer} while simultaneously persisting them to the
         * file system.
         */
        private static final class LogWorker implements Runnable, Closeable {

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
            private final ArrayBlockingQueue<Event> writeBuffer;

            /**
             * Outgoing {@link Event} buffer shared across all workers on a
             * {@link PersistedQueue.Local}.
             */
            private final ArrayBlockingQueue<Event> readBuffer;

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
             * Offset on the backing file to read the next {@link Event} from in case
             * {@link PersistedQueue.Local.LogWorker#outBuffer} is depleted.<br />
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
            private int count;

            /**
             * Number of {@link Event} successfully passed to
             * {@link PersistedQueue.Local.LogWorker#readBuffer}.
             */
            private int flushed;

            /**
             * Ctor.
             * @param file Backing data {@link File}
             * @param readBuffer Same instance as {@link PersistedQueue.Local#readBuffer}
             * @param writeBuffer Same instance as {@link PersistedQueue.Local#writeBuffer}
             * @param ack Maximum number of in-flight events, that are either stored serialized
             * in {@link PersistedQueue.Local.LogWorker#obuf} or already written to
             * {@link PersistedQueue.Local.LogWorker#out}, but not yet `fsync`ed to the file system
             * @throws IOException On failure to open backing data file for either reads or writes
             */
            LogWorker(final File file, final ArrayBlockingQueue<Event> readBuffer,
                final ArrayBlockingQueue<Event> writeBuffer, final int ack) throws IOException {
                this.file = new FileOutputStream(file);
                this.fd = this.file.getFD();
                this.out = this.file.getChannel();
                this.in = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                this.readBuffer = readBuffer;
                this.writeBuffer = writeBuffer;
                count = 0;
                flushed = 0;
                highWatermarkPos = 0L;
                watermarkPos = 0L;
                this.ack = ack / 2;
            }

            @Override
            public void run() {
                while (running.get()) {
                    try {
                        final Event event = this.writeBuffer.poll(10L, TimeUnit.MILLISECONDS);
                        final boolean fullyRead =
                            highWatermarkPos + obuf.position() == this.watermarkPos;
                        if (event != null) {
                            write(event);
                            if (count == flushed - 1 && this.readBuffer.offer(event)) {
                                this.completeWatermark();
                            } else {
                                if (fullyRead && outBuffer.offer(event)) {
                                    this.completeWatermark();
                                }
                            }
                        }
                        if (count % ack == 0 && obuf.position() > 0) {
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

            public void shutdown() {
                this.running.set(false);
            }

            public void awaitShutdown() {
                try {
                    this.shutdown.await();
                } catch (final InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
            }

            @Override
            public void close() throws IOException {
                this.awaitShutdown();
                this.in.close();
                this.file.close();
            }

            /**
             * Sets the watermark for number of bytes processed to
             */
            private void completeWatermark() {
                watermarkPos = highWatermarkPos + obuf.position();
            }

            private void write(final Queueable event) throws IOException {
                ++count;
                final byte[] data = event.serialize();
                maybeFlush(data.length + Integer.BYTES);
                obuf.putInt(data.length);
                obuf.put(data);
            }

            private void maybeFlush(final int size) throws IOException {
                if (obuf.position() > 0 && obuf.remaining() < size) {
                    flush();
                }
            }

            private void flush() throws IOException {
                obuf.flip();
                highWatermarkPos += (long) out.write(obuf);
                fd.sync();
                obuf.clear();
            }

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

            private boolean advanceFile() throws IOException {
                if (flushed + outBuffer.size() < count &&
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
    }
}
