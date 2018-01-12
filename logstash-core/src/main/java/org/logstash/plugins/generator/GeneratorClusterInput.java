package org.logstash.plugins.generator;

import org.logstash.Event;
import org.logstash.FieldReference;
import org.logstash.RubyUtil;
import org.logstash.cluster.ClusterInput;
import org.logstash.ext.JrubyEventExtLibrary;

public final class GeneratorClusterInput implements Runnable {

    private final ClusterInput cluster;

    public GeneratorClusterInput(final ClusterInput cluster) {
        this.cluster = cluster;
    }

    @Override
    public void run() {
        cluster.getTasks().pushTask(
            (server, events) -> {
                for (int i = 0; i < 10; ++i) {
                    final JrubyEventExtLibrary.RubyEvent event =
                        JrubyEventExtLibrary.RubyEvent.newRubyEvent(RubyUtil.RUBY, new Event());
                    event.getEvent().setField(FieldReference.from("message"), "generated");
                    events.push(event);
                }
            }
        );
    }
}
