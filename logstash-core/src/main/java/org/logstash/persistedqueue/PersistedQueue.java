package org.logstash.persistedqueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.logstash.Event;

public interface PersistedQueue extends Closeable {

    void enqueue(final Event event) throws InterruptedException;

    Event dequeue() throws InterruptedException;

    Event poll(long timeout, TimeUnit unit) throws InterruptedException;

    class Local implements PersistedQueue {

        private final ExecutorService exec = Executors.newSingleThreadExecutor();

        private final ArrayBlockingQueue<Event> buffer;

        public Local(final int ack, final String directory) {
            this.buffer = new ArrayBlockingQueue<>(ack);
        }

        @Override
        public void enqueue(final Event event) throws InterruptedException {
            buffer.put(event);
        }

        @Override
        public Event dequeue() throws InterruptedException {
            return buffer.take();
        }

        @Override
        public Event poll(final long timeout, final TimeUnit unit) throws InterruptedException {
            return buffer.poll(timeout, unit);
        }

        @Override
        public void close() throws IOException {
            exec.shutdown();
        }
    }
}
