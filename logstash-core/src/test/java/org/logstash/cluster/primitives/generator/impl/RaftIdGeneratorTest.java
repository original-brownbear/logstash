package org.logstash.cluster.primitives.generator.impl;

import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounter;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterService;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitiveTest;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.service.RaftService;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@code AtomixIdGenerator}.
 */
public class RaftIdGeneratorTest extends AbstractRaftPrimitiveTest<RaftAtomicCounter> {

    @Override
    protected RaftService createService() {
        return new RaftAtomicCounterService();
    }

    @Override
    protected RaftAtomicCounter createPrimitive(RaftProxy proxy) {
        return new RaftAtomicCounter(proxy);
    }

    /**
     * Tests generating IDs.
     */
    @Test
    public void testNextId() {
        RaftIdGenerator idGenerator1 = new RaftIdGenerator(newPrimitive("testNextId"));
        RaftIdGenerator idGenerator2 = new RaftIdGenerator(newPrimitive("testNextId"));

        CompletableFuture<Long> future11 = idGenerator1.nextId();
        CompletableFuture<Long> future12 = idGenerator1.nextId();
        CompletableFuture<Long> future13 = idGenerator1.nextId();
        assertEquals(Long.valueOf(1), future11.join());
        assertEquals(Long.valueOf(2), future12.join());
        assertEquals(Long.valueOf(3), future13.join());

        CompletableFuture<Long> future21 = idGenerator1.nextId();
        CompletableFuture<Long> future22 = idGenerator1.nextId();
        CompletableFuture<Long> future23 = idGenerator1.nextId();
        assertEquals(Long.valueOf(6), future23.join());
        assertEquals(Long.valueOf(5), future22.join());
        assertEquals(Long.valueOf(4), future21.join());

        CompletableFuture<Long> future31 = idGenerator2.nextId();
        CompletableFuture<Long> future32 = idGenerator2.nextId();
        CompletableFuture<Long> future33 = idGenerator2.nextId();
        assertEquals(Long.valueOf(1001), future31.join());
        assertEquals(Long.valueOf(1002), future32.join());
        assertEquals(Long.valueOf(1003), future33.join());
    }

    /**
     * Tests generating IDs.
     */
    @Test
    public void testNextIdBatchRollover() {
        RaftIdGenerator idGenerator1 = new RaftIdGenerator(newPrimitive("testNextIdBatchRollover"), 2);
        RaftIdGenerator idGenerator2 = new RaftIdGenerator(newPrimitive("testNextIdBatchRollover"), 2);

        CompletableFuture<Long> future11 = idGenerator1.nextId();
        CompletableFuture<Long> future12 = idGenerator1.nextId();
        CompletableFuture<Long> future13 = idGenerator1.nextId();
        assertEquals(Long.valueOf(1), future11.join());
        assertEquals(Long.valueOf(2), future12.join());
        assertEquals(Long.valueOf(3), future13.join());

        CompletableFuture<Long> future21 = idGenerator2.nextId();
        CompletableFuture<Long> future22 = idGenerator2.nextId();
        CompletableFuture<Long> future23 = idGenerator2.nextId();
        assertEquals(Long.valueOf(5), future21.join());
        assertEquals(Long.valueOf(6), future22.join());
        assertEquals(Long.valueOf(7), future23.join());

        CompletableFuture<Long> future14 = idGenerator1.nextId();
        CompletableFuture<Long> future15 = idGenerator1.nextId();
        CompletableFuture<Long> future16 = idGenerator1.nextId();
        assertEquals(Long.valueOf(4), future14.join());
        assertEquals(Long.valueOf(9), future15.join());
        assertEquals(Long.valueOf(10), future16.join());
    }
}
