package org.logstash.cluster.protocols.raft.storage.snapshot;

import java.io.File;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Snapshot file test.
 */
public final class SnapshotFileTest {

    /**
     * Tests creating a snapshot file name.
     */
    @Test
    public void testCreateSnapshotFileName() {
        assertEquals(SnapshotFile.createSnapshotFileName("foo", 1, 2), "foo-1-2.snapshot");
        assertEquals(SnapshotFile.createSnapshotFileName("foo-bar", 1, 2), "foo-bar-1-2.snapshot");
    }

    /**
     * Tests determining whether a file is a snapshot file.
     */
    @Test
    public void testCreateValidateSnapshotFile() {
        assertTrue(SnapshotFile.isSnapshotFile(SnapshotFile.createSnapshotFile(new File(System.getProperty("user.dir")), "foo", 1, 2)));
        assertTrue(SnapshotFile.isSnapshotFile(SnapshotFile.createSnapshotFile(new File(System.getProperty("user.dir")), "foo-bar", 1, 2)));
    }

    @Test
    public void testParseSnapshotName() {
        assertEquals("foo", SnapshotFile.parseName("foo-1-2.snapshot"));
        assertEquals("foo-bar", SnapshotFile.parseName("foo-bar-1-2.snapshot"));
    }

}
