package org.logstash.persistedqueue;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.Event;

public final class PersistedQueueTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void persistsToDisk() throws Exception {
        final File dir = temp.newFolder();
        final ExecutorService exec = Executors.newSingleThreadExecutor();
        try (PersistedQueue queue = new PersistedQueue.Local(1, dir.getAbsolutePath())) {
            final Future<?> future = exec.submit(() -> {
                try {
                    for (int i = 0; i < 1_000_000; ++i) {
                        MatcherAssert
                            .assertThat(queue.dequeue().getField("foo"), CoreMatchers.is(i));
                    }
                } catch (final InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
            });
            for (int i = 0; i < 1_000_000; ++i) {
                final Event event = new Event();
                event.setField("foo", i);
                queue.enqueue(event);
            }
            future.get();
        }
    }
}
