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
package org.logstash.cluster.protocols.raft.storage.snapshot;

import java.io.File;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Snapshot file test.
 */
public class SnapshotFileTest {

    /**
     * Tests creating a snapshot file name.
     */
    @Test
    public void testCreateSnapshotFileName() throws Exception {
        assertEquals(SnapshotFile.createSnapshotFileName("foo", 1, 2), "foo-1-2.snapshot");
        assertEquals(SnapshotFile.createSnapshotFileName("foo-bar", 1, 2), "foo-bar-1-2.snapshot");
    }

    /**
     * Tests determining whether a file is a snapshot file.
     */
    @Test
    public void testCreateValidateSnapshotFile() throws Exception {
        assertTrue(SnapshotFile.isSnapshotFile(SnapshotFile.createSnapshotFile(new File(System.getProperty("user.dir")), "foo", 1, 2)));
        assertTrue(SnapshotFile.isSnapshotFile(SnapshotFile.createSnapshotFile(new File(System.getProperty("user.dir")), "foo-bar", 1, 2)));
    }

    @Test
    public void testParseSnapshotName() throws Exception {
        assertEquals("foo", SnapshotFile.parseName("foo-1-2.snapshot"));
        assertEquals("foo-bar", SnapshotFile.parseName("foo-bar-1-2.snapshot"));
    }

}
