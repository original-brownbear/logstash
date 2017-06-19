package org.logstash.persistedqueue;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
    
    private static final String FIELD = "foo";

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void persistsToDisk() throws Exception {
        final File dir = temp.newFolder();
        final ExecutorService exec = Executors.newSingleThreadExecutor();
        final int count = 1_000_000;
        try (PersistedQueue queue = new PersistedQueue.Local(1024, dir.getAbsolutePath())) {
            final Future<?> future = exec.submit(() -> verifyEvents(count, queue));
            enqueueEvents(count, queue);
            future.get();
        }
    }

    @Test
    public void persistsToDiskAckOne() throws Exception {
        final File dir = temp.newFolder();
        final ExecutorService exec = Executors.newSingleThreadExecutor();
        final int count = 100_000;
        try (PersistedQueue queue = new PersistedQueue.Local(1, dir.getAbsolutePath())) {
            final Future<?> future = exec.submit(() -> verifyEvents(count, queue));
            enqueueEvents(count, queue);
            future.get();
        }
    }

    @Test
    public void reusesDirectory() throws Exception {
        final File dir = temp.newFolder();
        final ExecutorService exec = Executors.newSingleThreadExecutor();
        final int count = 10_000;
        for (int j = 0; j < 3; ++j) {
            try (PersistedQueue queue = new PersistedQueue.Local(1024, dir.getAbsolutePath())) {
                final Future<?> future = exec.submit(() -> verifyEvents(count, queue));
                enqueueEvents(count, queue);
                future.get();
            }
        }
    }

    @Test
    public void couldRewind() throws Exception {
        final File dir = temp.newFolder();
        final int count = 10_000;
        try (PersistedQueue queue = new PersistedQueue.Local(1024, dir.getAbsolutePath())) {
            enqueueEvents(count, queue);
            verifyEvents(count, queue);
        }
        final Path indexPath = dir.toPath().resolve("queue.index");
        try (final DataOutputStream data = 
                 new DataOutputStream(new FileOutputStream(indexPath.toFile(), true))
        ) {
            final byte[] index = Files.readAllBytes(indexPath);
            data.writeInt(0);
            data.writeLong(0L);
            data.write(index, index.length - Long.BYTES, Long.BYTES);
            data.flush();
        }
        try (PersistedQueue queue = new PersistedQueue.Local(1024, dir.getAbsolutePath())) {
            verifyEvents(count, queue);
        }
    }

    private static void enqueueEvents(final int count, final PersistedQueue queue) {
        for (int i = 0; i < count; ++i) {
            final Event event = new Event();
            event.setField(FIELD, i);
            try {
                queue.enqueue(event);
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    private static void verifyEvents(final int count, final PersistedQueue queue) {
        for (int i = 0; i < count; ++i) {
            try {
                assertThat(queue.dequeue().getField(FIELD), is(i));
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }
}
