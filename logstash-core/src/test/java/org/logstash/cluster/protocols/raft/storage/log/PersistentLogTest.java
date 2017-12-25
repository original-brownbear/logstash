package org.logstash.cluster.protocols.raft.storage.log;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Disk log test.
 */
public abstract class PersistentLogTest extends AbstractLogTest {

    /**
     * Tests reading from a compacted log.
     */
    @Test
    public void testCompactAndRecover() {
        RaftLog log = createLog();

        // Write three segments to the log.
        RaftLogWriter writer = log.writer();
        for (int i = 0; i < MAX_ENTRIES_PER_SEGMENT * 3; i++) {
            writer.append(new TestEntry(1, 1));
        }

        // Commit the entries and compact the first segment.
        writer.commit(MAX_ENTRIES_PER_SEGMENT * 3);
        log.compact(MAX_ENTRIES_PER_SEGMENT + 1);

        // Close the log.
        log.close();

        // Reopen the log and create a reader.
        log = createLog();
        writer = log.writer();
        RaftLogReader reader = log.openReader(1, RaftLogReader.Mode.COMMITS);
        writer.append(new TestEntry(1, 1));
        writer.append(new TestEntry(1, 1));
        writer.commit(MAX_ENTRIES_PER_SEGMENT * 3);

        // Ensure the reader starts at the first physical index in the log.
        assertEquals(MAX_ENTRIES_PER_SEGMENT + 1, reader.getNextIndex());
        assertEquals(reader.getFirstIndex(), reader.getNextIndex());
        assertTrue(reader.hasNext());
        assertEquals(MAX_ENTRIES_PER_SEGMENT + 1, reader.getNextIndex());
        assertEquals(reader.getFirstIndex(), reader.getNextIndex());
        assertEquals(MAX_ENTRIES_PER_SEGMENT + 1, reader.next().index());
    }
}
