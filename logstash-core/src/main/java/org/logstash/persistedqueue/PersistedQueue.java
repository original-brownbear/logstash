package org.logstash.persistedqueue;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
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

        private final ExecutorService exec = Executors.newSingleThreadExecutor();

        private final LogWorker worker;

        private final ArrayBlockingQueue<Event> writeBuffer;

        private final ArrayBlockingQueue<Event> readBuffer;

        public Local(final int ack, final String directory) throws FileNotFoundException {
            this.writeBuffer = new ArrayBlockingQueue<>(ack);
            this.readBuffer = new ArrayBlockingQueue<>(1024);
            this.worker = new PersistedQueue.Local.LogWorker(
                Paths.get(directory, "0.log").toFile(),
                readBuffer, writeBuffer
            );
            this.exec.execute(worker);
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
            return writeBuffer.poll(timeout, unit);
        }

        @Override
        public void close() throws IOException {
            this.worker.shutdown();
            this.worker.awaitShutdown();
            exec.shutdown();
        }

        private static final class LogWorker implements Runnable {

            private final FileOutputStream file;

            private final FileChannel out;

            private final ByteBuffer iobuf = ByteBuffer.allocateDirect(256 * 256);

            private final ArrayBlockingQueue<Event> writeBuffer;

            private final ArrayBlockingQueue<Event> readBuffer;

            private final CountDownLatch shutdown = new CountDownLatch(1);

            private final AtomicBoolean running = new AtomicBoolean(true);

            private final AtomicLong pressure = new AtomicLong(0);

            LogWorker(final File file, final ArrayBlockingQueue<Event> readBuffer,
                final ArrayBlockingQueue<Event> writeBuffer) throws FileNotFoundException {
                this.file = new FileOutputStream(file);
                this.out = this.file.getChannel();
                this.readBuffer = readBuffer;
                this.writeBuffer = writeBuffer;
            }

            @Override
            public void run() {
                while (running.get()) {
                    try {
                        final Event event = this.writeBuffer.poll(1L, TimeUnit.SECONDS);
                        if(event != null) {
                            write(this.writeBuffer.take());
                            if (!this.readBuffer.offer(event)) {
                                pressure.incrementAndGet();
                            }
                        }
                    } catch (final InterruptedException | IOException ex) {
                        throw new IllegalStateException(ex);
                    }
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
                final byte[] data = event.serialize();
                iobuf.clear();
                iobuf.putInt(data.length);
                iobuf.put(data);
                iobuf.flip();
                out.write(iobuf);
                this.file.getFD().sync();
            }
        }
    }
}
