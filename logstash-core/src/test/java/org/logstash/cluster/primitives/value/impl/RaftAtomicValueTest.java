package org.logstash.cluster.primitives.value.impl;

import org.junit.Test;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitiveTest;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.service.RaftService;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Raft atomic value test.
 */
public class RaftAtomicValueTest extends AbstractRaftPrimitiveTest<RaftAtomicValue> {
    @Override
    protected RaftService createService() {
        return new RaftAtomicValueService();
    }

    @Override
    protected RaftAtomicValue createPrimitive(RaftProxy proxy) {
        return new RaftAtomicValue(proxy);
    }

    @Test
    public void testValue() {
        byte[] bytes1 = "a".getBytes();
        byte[] bytes2 = "b".getBytes();
        byte[] bytes3 = "c".getBytes();

        RaftAtomicValue value = newPrimitive("test-value");
        assertEquals(0, value.get().join().length);
        value.set(bytes1).join();
        assertArrayEquals(bytes1, value.get().join());
        assertFalse(value.compareAndSet(bytes2, bytes3).join());
        assertTrue(value.compareAndSet(bytes1, bytes2).join());
        assertArrayEquals(bytes2, value.get().join());
        assertArrayEquals(bytes2, value.getAndSet(bytes3).join());
        assertArrayEquals(bytes3, value.get().join());
    }
}
