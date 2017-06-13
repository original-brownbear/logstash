package org.logstash.persistedqueue;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.Event;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public final class PersistedQueueTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void persistsToDisk() throws Exception {
        final File dir = temp.newFolder();
        final ExecutorService exec = Executors.newSingleThreadExecutor();
        final int count = 1_000_000_0;
        try (PersistedQueue queue = new PersistedQueue.Local(1024, dir.getAbsolutePath())) {
            final Future<?> future = exec.submit(() -> {
                try {
                    for (int i = 0; i < count; ++i) {
                        assertThat(queue.dequeue().getField("foo"), is(i));
                    }
                } catch (final InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
            });
            for (int i = 0; i < count; ++i) {
                final Event event = new Event();
                event.setField("foo", i);
                queue.enqueue(event);
            }
            future.get();
        }
    }
}
