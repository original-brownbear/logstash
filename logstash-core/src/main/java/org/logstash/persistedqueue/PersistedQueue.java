package org.logstash.persistedqueue;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.logstash.Event;

public interface PersistedQueue extends Closeable {

    void enqueue(Event event) throws InterruptedException;

    Event dequeue() throws InterruptedException;

    Event poll(long timeout, TimeUnit unit) throws InterruptedException;

    class Local implements PersistedQueue {

        private static final int CONCURRENT = 1;

        private final ExecutorService exec = Executors.newFixedThreadPool(CONCURRENT);

        private final PersistedQueue.Local.LogWorker[] workers =
            new PersistedQueue.Local.LogWorker[CONCURRENT];

        private final ArrayBlockingQueue<Event> writeBuffer;

        private final ArrayBlockingQueue<Event> readBuffer;

        public Local(final int ack, final String directory) {
            this.writeBuffer = new ArrayBlockingQueue<>(ack);
            this.readBuffer = new ArrayBlockingQueue<>(1024);
            for (int i = 0; i < CONCURRENT; ++i) {
                try {
                    this.workers[i] = new PersistedQueue.Local.LogWorker(
                        Paths.get(directory, String.format("%d.log", i)).toFile(),
                        readBuffer, writeBuffer
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
                this.workers[i].awaitShutdown();
            }
            exec.shutdown();
        }

        private static final class LogWorker implements Runnable {

            private static final int ACK_INTERVAL = 1024;

            private static final int OUT_BUFFER_SIZE = 10;

            private final FileOutputStream file;

            private final FileChannel out;

            private final FileChannel in;

            private final ByteBuffer obuf = ByteBuffer.allocateDirect(256 * 256);

            private final ByteBuffer ibuf = ByteBuffer.allocateDirect(256 * 256);

            private final ArrayBlockingQueue<Event> writeBuffer;

            private final ArrayBlockingQueue<Event> readBuffer;

            private final CountDownLatch shutdown = new CountDownLatch(1);

            private final AtomicBoolean running = new AtomicBoolean(true);

            private final AtomicLong watermarkPos = new AtomicLong(0L);

            private final ArrayDeque<Event> outBuffer = new ArrayDeque<>(OUT_BUFFER_SIZE);

            private int count;

            private int flushed;

            LogWorker(final File file, final ArrayBlockingQueue<Event> readBuffer,
                final ArrayBlockingQueue<Event> writeBuffer) throws IOException {
                this.file = new FileOutputStream(file);
                this.out = this.file.getChannel();
                this.in = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                this.readBuffer = readBuffer;
                this.writeBuffer = writeBuffer;
                count = 0;
                flushed = 0;
            }

            @Override
            public void run() {
                while (running.get()) {
                    try {
                        final Event event = this.writeBuffer.poll(10L, TimeUnit.MILLISECONDS);
                        final boolean fullyRead =
                            out.position() + obuf.position() == this.watermarkPos.get();
                        if (event != null) {
                            write(event);
                            if (count == flushed - 1 && this.readBuffer.offer(event)) {
                                watermarkPos.set(out.position() + obuf.position());
                            } else {
                                if (fullyRead && outBuffer.size() < OUT_BUFFER_SIZE) {
                                    outBuffer.add(event);
                                    watermarkPos.set(out.position() + obuf.position());
                                }
                            }
                        }
                        if (obuf.position() > 0 && count % ACK_INTERVAL == 0) {
                            flush();
                        }
                        while (advanceFile() || advanceBuffers()) {
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
                out.write(obuf);
                this.file.getFD().sync();
                obuf.clear();
            }

            private boolean advanceBuffers() {
                final boolean result;
                Event e;
                if ((e = outBuffer.peek()) != null && readBuffer.offer(e)) {
                    outBuffer.pop();
                    flushed++;
                    result = true;
                } else {
                    result = false;
                }
                return result;
            }

            private boolean advanceFile() throws IOException {
                final boolean result;
                if (flushed + outBuffer.size() < count &&
                    this.watermarkPos.get() == this.out.position()) {
                    this.flush();
                }
                if (outBuffer.size() < OUT_BUFFER_SIZE &&
                    this.watermarkPos.get() < this.out.position()) {
                    this.in.position(watermarkPos.get());
                    ibuf.clear();
                    this.in.read(ibuf);
                    ibuf.flip();
                    int len = ibuf.getInt();
                    final byte[] data = new byte[len];
                    ibuf.get(data);
                    outBuffer.add(Event.deserialize(data));
                    this.watermarkPos.addAndGet(len + Integer.BYTES);
                    result = true;
                } else {
                    result = false;
                }
                return result;
            }
        }
    }
}
