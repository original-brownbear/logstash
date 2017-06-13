package org.logstash.persistedqueue;

import java.io.File;
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
        try (PersistedQueue queue = new PersistedQueue.Local(1, dir.getAbsolutePath())) {
            for (int i = 0; i < 1_000_000; ++i) {
                final Event event = new Event();
                event.setField("foo", i);
                queue.enqueue(event);
            }
            for (int i = 0; i < 1_000_000; ++i) {
                MatcherAssert.assertThat(queue.dequeue().getField("foo"), CoreMatchers.is(i));
            }
        }
    }
}
