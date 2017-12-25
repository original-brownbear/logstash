package org.logstash.cluster.protocols.raft.storage;

import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Raft storage test.
 */
public final class RaftStorageTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testDefaultConfiguration() {
        RaftStorage storage = RaftStorage.builder().build();
        assertEquals("atomix", storage.prefix());
        assertEquals(new File(System.getProperty("user.dir")), storage.directory());
        assertEquals(1024 * 1024 * 32, storage.maxLogSegmentSize());
        assertEquals(1024 * 1024, storage.maxLogEntriesPerSegment());
        assertTrue(storage.dynamicCompaction());
        assertEquals(.2, storage.freeDiskBuffer(), .01);
        assertFalse(storage.isFlushOnCommit());
        assertFalse(storage.isRetainStaleSnapshots());
    }

    @Test
    public void testCustomConfiguration() throws Exception {
        final File directory = temporaryFolder.newFolder();
        RaftStorage storage = RaftStorage.builder()
            .withPrefix("foo")
            .withDirectory(directory)
            .withMaxSegmentSize(1024 * 1024)
            .withMaxEntriesPerSegment(1024)
            .withDynamicCompaction(false)
            .withFreeDiskBuffer(.5)
            .withFlushOnCommit()
            .withRetainStaleSnapshots()
            .build();
        assertEquals("foo", storage.prefix());
        assertEquals(directory, storage.directory());
        assertEquals(1024 * 1024, storage.maxLogSegmentSize());
        assertEquals(1024, storage.maxLogEntriesPerSegment());
        assertEquals(.5, storage.freeDiskBuffer(), .01);
        assertTrue(storage.isFlushOnCommit());
        assertTrue(storage.isRetainStaleSnapshots());
    }
}
