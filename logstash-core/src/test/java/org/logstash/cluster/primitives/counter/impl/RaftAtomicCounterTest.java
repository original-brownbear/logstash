package org.logstash.cluster.primitives.counter.impl;

import org.junit.Test;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitiveTest;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.service.RaftService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RaftAtomicCounter}.
 */
public class RaftAtomicCounterTest extends AbstractRaftPrimitiveTest<RaftAtomicCounter> {
    @Override
    protected RaftService createService() {
        return new RaftAtomicCounterService();
    }

    @Override
    protected RaftAtomicCounter createPrimitive(RaftProxy proxy) {
        return new RaftAtomicCounter(proxy);
    }

    @Test
    public void testBasicOperations() {
        basicOperationsTest();
    }

    protected void basicOperationsTest() {
        RaftAtomicCounter along = newPrimitive("test-counter-basic-operations");
        assertEquals(0, along.get().join().longValue());
        assertEquals(1, along.incrementAndGet().join().longValue());
        along.set(100).join();
        assertEquals(100, along.get().join().longValue());
        assertEquals(100, along.getAndAdd(10).join().longValue());
        assertEquals(110, along.get().join().longValue());
        assertFalse(along.compareAndSet(109, 111).join());
        assertTrue(along.compareAndSet(110, 111).join());
        assertEquals(100, along.addAndGet(-11).join().longValue());
        assertEquals(100, along.getAndIncrement().join().longValue());
        assertEquals(101, along.get().join().longValue());
    }
}
