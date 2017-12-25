package org.logstash.cluster.protocols.raft.storage.snapshot;

import org.junit.Test;
import org.logstash.cluster.storage.buffer.Buffer;
import org.logstash.cluster.storage.buffer.HeapBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Snapshot descriptor test.
 */
public class SnapshotDescriptorTest {

    @Test
    public void testSnapshotDescriptor() {
        SnapshotDescriptor descriptor = SnapshotDescriptor.builder()
            .withServiceId(1)
            .withIndex(2)
            .withTimestamp(3)
            .build();
        assertEquals(1, descriptor.serviceId());
        assertEquals(2, descriptor.index());
        assertEquals(3, descriptor.timestamp());
    }

    @Test
    public void testCopySnapshotDescriptor() {
        SnapshotDescriptor descriptor = SnapshotDescriptor.builder()
            .withServiceId(1)
            .withIndex(2)
            .withTimestamp(3)
            .build();
        Buffer buffer = HeapBuffer.allocate(SnapshotDescriptor.BYTES);
        descriptor.copyTo(buffer);
        buffer.flip();
        descriptor = new SnapshotDescriptor(buffer);
        assertEquals(1, descriptor.serviceId());
        assertEquals(2, descriptor.index());
        assertEquals(3, descriptor.timestamp());
    }

}
