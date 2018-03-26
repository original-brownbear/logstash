package org.logstash.execution.queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.logstash.Event;
import org.logstash.ext.JrubyEventExtLibrary;

public final class LegacyMemoryQueueReader implements QueueReader {

    private final AtomicLong sequencer = new AtomicLong(0L);

    private final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue;

    public LegacyMemoryQueueReader(final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue) {
        this.queue = queue;
    }

    @Override
    public long poll(final Event event) {
        try {
            event.overwrite(queue.take().getEvent());
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
        return sequencer.incrementAndGet();
    }

    @Override
    public long poll(final Event event, final long millis) {
        final long sequence;
        try {
            final JrubyEventExtLibrary.RubyEvent rubyEvent =
                queue.poll(millis, TimeUnit.MILLISECONDS);
            if (rubyEvent == null) {
                sequence = -1L;
            } else {
                event.overwrite(rubyEvent.getEvent());
                sequence = sequencer.incrementAndGet();
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
        return sequence;
    }

    @Override
    public void acknowledge(final long sequenceNum) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
