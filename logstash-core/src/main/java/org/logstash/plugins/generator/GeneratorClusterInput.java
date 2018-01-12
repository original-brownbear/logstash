package org.logstash.plugins.generator;

import java.util.concurrent.atomic.AtomicInteger;
import org.logstash.Event;
import org.logstash.FieldReference;
import org.logstash.RubyUtil;
import org.logstash.cluster.ClusterInput;
import org.logstash.ext.JrubyEventExtLibrary;

public final class GeneratorClusterInput implements Runnable {

    private static final AtomicInteger CHUNK_SEQUENCER = new AtomicInteger();

    private final ClusterInput cluster;

    public GeneratorClusterInput(final ClusterInput cluster) {
        this.cluster = cluster;
    }

    @Override
    public void run() {
        final String message = String.format("generated %d", CHUNK_SEQUENCER.incrementAndGet());
        cluster.getTasks().pushTask(
            (server, events) -> {
                for (int i = 0; i < 10; ++i) {
                    final JrubyEventExtLibrary.RubyEvent event =
                        JrubyEventExtLibrary.RubyEvent.newRubyEvent(RubyUtil.RUBY, new Event());
                    event.getEvent().setField(FieldReference.from("message"), message);
                    events.push(event);
                }
            }
        );
    }
}
