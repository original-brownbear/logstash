package org.logstash.persistedqueue;

import java.io.File;
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
        final Event event = new Event();
        event.setField("foo", "bar");
        try (PersistedQueue queue = new PersistedQueue.Local(1, dir.getAbsolutePath())) {
            for (int i = 0; i < 1_000_000; ++i) {
                queue.enqueue(event);
            }
        }
    }
}
