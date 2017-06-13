package org.logstash.persistedqueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import org.logstash.Event;

public interface PersistedQueue extends Closeable {

    void enqueue(final Event event) throws InterruptedException;

    Event dequeue() throws InterruptedException;

    class Local implements PersistedQueue {

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
        public void close() throws IOException {
        }
    }
}
