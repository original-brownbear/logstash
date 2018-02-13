package org.logstash.execution;

import org.logstash.Event;

/**
 * A Filter is simply a mapping of {@link QueueReader} to a new {@link QueueReader}.
 */
public interface Filter extends AutoCloseable {

    QueueReader filter(QueueReader reader);

    void flush(boolean isShutdown);

    @LogstashPlugin(name = "mutate")
    final class Mutate implements Filter {

        private final String field;

        private final String value;

        /**
         * Required Constructor Signature only taking a {@link LsConfiguration}.
         * @param configuration Logstash Configuration
         * @param context Logstash Context
         */
        public Mutate(final LsConfiguration configuration, final LsContext context) {
            this.field = configuration.getString("ls.plugin.mutate.field");
            this.value = configuration.getString("ls.plugin.mutate.value");
        }

        @Override
        public QueueReader filter(final QueueReader reader) {
            return new QueueReader() {
                @Override
                public long poll(final Event event) {
                    final long seq = reader.poll(event);
                    if (seq > -1L) {
                        event.setField(field, value);
                    }
                    return seq;
                }

                @Override
                public long poll(final Event event, final long millis) {
                    final long seq = reader.poll(event, millis);
                    if (seq > -1L) {
                        event.setField(field, value);
                    }
                    return seq;
                }

                @Override
                public void acknowledge(final long sequenceNum) {
                    reader.acknowledge(sequenceNum);
                }
            };
        }

        @Override
        public void flush(final boolean isShutdown) {
            // Nothing to do here
        }

        @Override
        public void close() {
            // Nothing to do here
        }
    }
}
