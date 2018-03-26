package org.logstash.execution.queue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.logstash.Event;
import org.logstash.RubyUtil;
import org.logstash.ext.JrubyEventExtLibrary;

public final class LegacyMemoryQueueWriter implements QueueWriter {

    private final AtomicLong sequencer = new AtomicLong(0L);

    private final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue;

    public LegacyMemoryQueueWriter(final BlockingQueue<JrubyEventExtLibrary.RubyEvent> queue) {
        this.queue = queue;
    }

    @Override
    public long push(final Map<String, Object> event) {
        try {
            queue.put(JrubyEventExtLibrary.RubyEvent.newRubyEvent(RubyUtil.RUBY, new Event(event)));
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
        return sequencer.incrementAndGet();
    }

    @Override
    public long watermark() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long highWatermark() {
        return sequencer.get();
    }
}
