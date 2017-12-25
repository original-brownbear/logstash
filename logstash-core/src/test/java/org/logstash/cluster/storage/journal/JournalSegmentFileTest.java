package org.logstash.cluster.storage.journal;

import java.io.File;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Journal segment file test.
 */
public class JournalSegmentFileTest {

    @Test
    public void testIsSegmentFile() {
        assertTrue(JournalSegmentFile.isSegmentFile("foo", "foo-1.log"));
        assertFalse(JournalSegmentFile.isSegmentFile("foo", "bar-1.log"));
        assertTrue(JournalSegmentFile.isSegmentFile("foo", "foo-1-1.log"));
    }

    @Test
    public void testCreateSegmentFile() {
        File file = JournalSegmentFile.createSegmentFile("foo", new File(System.getProperty("user.dir")), 1);
        assertTrue(JournalSegmentFile.isSegmentFile("foo", file));
    }

}
