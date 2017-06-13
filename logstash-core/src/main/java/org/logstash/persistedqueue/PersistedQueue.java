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

public interface PersistedQueue extends Closeable {

    /**
     * 
     * @param event Event to Enqueue
     * @throws InterruptedException
     */
    void enqueue(Event event) throws InterruptedException;

    Event dequeue() throws InterruptedException;

    Event poll(long timeout, TimeUnit unit) throws InterruptedException;

    final class Local implements PersistedQueue {

        private static final int CONCURRENT = 1;

        private final ExecutorService exec = Executors.newFixedThreadPool(CONCURRENT);

        private final PersistedQueue.Local.LogWorker[] workers =
            new PersistedQueue.Local.LogWorker[CONCURRENT];

        private final ArrayBlockingQueue<Event> writeBuffer;

        private final ArrayBlockingQueue<Event> readBuffer;

        public Local(final int ack, final String directory) {
            this.writeBuffer = new ArrayBlockingQueue<>(ack / 2);
            this.readBuffer = new ArrayBlockingQueue<>(1024);
            for (int i = 0; i < CONCURRENT; ++i) {
                try {
                    this.workers[i] = new PersistedQueue.Local.LogWorker(
                        Paths.get(directory, String.format("%d.log", i)).toFile(),
                        readBuffer, writeBuffer, ack
                    );
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
                this.exec.execute(workers[i]);
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
        public void close() throws IOException {
            for (int i = 0; i < CONCURRENT; ++i) {
                this.workers[i].shutdown();
                this.workers[i].close();
            }
            exec.shutdown();
        }

        private static final class LogWorker implements Runnable, Closeable {

            private static final int OUT_BUFFER_SIZE = 100;

            private static final int BYTE_BUFFER_SIZE = 1 << 16;

            private final int ack;
            
            private final FileOutputStream file;

            private final FileDescriptor fd;

            private final FileChannel out;

            private final FileChannel in;

            private final ByteBuffer obuf = ByteBuffer.allocateDirect(BYTE_BUFFER_SIZE);

            private final ByteBuffer ibuf = ByteBuffer.allocateDirect(BYTE_BUFFER_SIZE);

            private final ArrayBlockingQueue<Event> writeBuffer;

            private final ArrayBlockingQueue<Event> readBuffer;

            private final CountDownLatch shutdown = new CountDownLatch(1);

            private final AtomicBoolean running = new AtomicBoolean(true);

            private long watermarkPos;

            private long highWatermarkPos;

            private final ArrayBlockingQueue<Event> outBuffer =
                new ArrayBlockingQueue<>(OUT_BUFFER_SIZE);

            private int count;

            private int flushed;
            
            private byte[] readByteBuffer = new byte[BYTE_BUFFER_SIZE];

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

            private void completeWatermark() {
                watermarkPos = highWatermarkPos + obuf.position();
            }

            private void write(final Event event) throws IOException {
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
                highWatermarkPos += out.write(obuf);
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
                if (remaining > 0 &&
                    this.watermarkPos < highWatermarkPos) {
                    this.in.position(watermarkPos);
                    ibuf.clear();
                    this.in.read(ibuf);
                    ibuf.flip();
                    while (ibuf.remaining() >= Integer.BYTES && remaining > 0) {
                        int len = ibuf.getInt();
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
