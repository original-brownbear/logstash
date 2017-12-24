package org.logstash.ext;

import java.util.concurrent.BlockingQueue;

public interface EventQueue {

    void push(JrubyEventExtLibrary.RubyEvent event);

    static EventQueue wrap(final BlockingQueue<JrubyEventExtLibrary.RubyEvent> java) {
        return event -> {
            try {
                java.put(event);
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(ex);
            }
        };
    }
}
