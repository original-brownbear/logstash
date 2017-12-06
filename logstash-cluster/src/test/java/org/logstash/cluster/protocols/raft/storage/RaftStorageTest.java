/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
