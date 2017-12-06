package org.logstash.cluster.primitives.queue.impl;

import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitiveTest;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.primitives.queue.WorkQueueStats;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.service.RaftService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RaftWorkQueue}.
 */
public class RaftWorkQueueTest extends AbstractRaftPrimitiveTest<RaftWorkQueue> {
    private static final Duration DEFAULT_PROCESSING_TIME = Duration.ofMillis(100);
    private static final byte[] DEFAULT_PAYLOAD = "hello world".getBytes();

    @Override
    protected RaftService createService() {
        return new RaftWorkQueueService();
    }

    @Override
    protected RaftWorkQueue createPrimitive(RaftProxy proxy) {
        return new RaftWorkQueue(proxy);
    }

    @Test
    public void testAdd() {
        String queueName = UUID.randomUUID().toString();
        RaftWorkQueue queue1 = newPrimitive(queueName);
        byte[] item = DEFAULT_PAYLOAD;
        queue1.addOne(item).join();

        RaftWorkQueue queue2 = newPrimitive(queueName);
        byte[] task2 = DEFAULT_PAYLOAD;
        queue2.addOne(task2).join();

        WorkQueueStats stats = queue1.stats().join();
        assertEquals(stats.totalPending(), 2);
        assertEquals(stats.totalInProgress(), 0);
        assertEquals(stats.totalCompleted(), 0);
    }

    @Test
    public void testAddMultiple() {
        String queueName = UUID.randomUUID().toString();
        RaftWorkQueue queue1 = newPrimitive(queueName);
        byte[] item1 = DEFAULT_PAYLOAD;
        byte[] item2 = DEFAULT_PAYLOAD;
        queue1.addMultiple(Arrays.asList(item1, item2)).join();

        WorkQueueStats stats = queue1.stats().join();
        assertEquals(stats.totalPending(), 2);
        assertEquals(stats.totalInProgress(), 0);
        assertEquals(stats.totalCompleted(), 0);
    }

    @Test
    public void testTakeAndComplete() {
        String queueName = UUID.randomUUID().toString();
        RaftWorkQueue queue1 = newPrimitive(queueName);
        byte[] item1 = DEFAULT_PAYLOAD;
        queue1.addOne(item1).join();

        RaftWorkQueue queue2 = newPrimitive(queueName);
        Task<byte[]> removedTask = queue2.take().join();

        WorkQueueStats stats = queue2.stats().join();
        assertEquals(stats.totalPending(), 0);
        assertEquals(stats.totalInProgress(), 1);
        assertEquals(stats.totalCompleted(), 0);

        assertTrue(Arrays.equals(removedTask.payload(), item1));
        queue2.complete(Arrays.asList(removedTask.taskId())).join();

        stats = queue1.stats().join();
        assertEquals(stats.totalPending(), 0);
        assertEquals(stats.totalInProgress(), 0);
        assertEquals(stats.totalCompleted(), 1);

        // Another take should return null
        assertNull(queue2.take().join());
    }

    @Test
    public void testUnexpectedClientClose() {
        String queueName = UUID.randomUUID().toString();
        RaftWorkQueue queue1 = newPrimitive(queueName);
        byte[] item1 = DEFAULT_PAYLOAD;
        queue1.addOne(item1).join();

        RaftWorkQueue queue2 = newPrimitive(queueName);
        queue2.take().join();

        WorkQueueStats stats = queue1.stats().join();
        assertEquals(0, stats.totalPending());
        assertEquals(1, stats.totalInProgress());
        assertEquals(0, stats.totalCompleted());

        queue2.close().join();

        stats = queue1.stats().join();
        assertEquals(1, stats.totalPending());
        assertEquals(0, stats.totalInProgress());
        assertEquals(0, stats.totalCompleted());
    }

    @Test
    public void testAutomaticTaskProcessing() throws Throwable {
        String queueName = UUID.randomUUID().toString();
        RaftWorkQueue queue1 = newPrimitive(queueName);
        Executor executor = Executors.newSingleThreadExecutor();

        CountDownLatch latch1 = new CountDownLatch(1);
        queue1.registerTaskProcessor(s -> latch1.countDown(), 2, executor);

        RaftWorkQueue queue2 = newPrimitive(queueName);
        byte[] item1 = DEFAULT_PAYLOAD;
        queue2.addOne(item1).join();

        assertTrue(Uninterruptibles.awaitUninterruptibly(latch1, 5000, TimeUnit.MILLISECONDS));
        queue1.stopProcessing();

        byte[] item2 = DEFAULT_PAYLOAD;
        byte[] item3 = DEFAULT_PAYLOAD;

        Thread.sleep((int) DEFAULT_PROCESSING_TIME.toMillis());

        queue2.addMultiple(Arrays.asList(item2, item3)).join();

        WorkQueueStats stats = queue1.stats().join();
        assertEquals(2, stats.totalPending());
        assertEquals(0, stats.totalInProgress());
        assertEquals(1, stats.totalCompleted());

        CountDownLatch latch2 = new CountDownLatch(2);
        queue1.registerTaskProcessor(s -> latch2.countDown(), 2, executor);

        Uninterruptibles.awaitUninterruptibly(latch2, 500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testDestroy() {
        String queueName = UUID.randomUUID().toString();
        RaftWorkQueue queue1 = newPrimitive(queueName);
        byte[] item = DEFAULT_PAYLOAD;
        queue1.addOne(item).join();

        RaftWorkQueue queue2 = newPrimitive(queueName);
        byte[] task2 = DEFAULT_PAYLOAD;
        queue2.addOne(task2).join();

        WorkQueueStats stats = queue1.stats().join();
        assertEquals(stats.totalPending(), 2);
        assertEquals(stats.totalInProgress(), 0);
        assertEquals(stats.totalCompleted(), 0);

        queue2.destroy().join();

        stats = queue1.stats().join();
        assertEquals(stats.totalPending(), 0);
        assertEquals(stats.totalInProgress(), 0);
        assertEquals(stats.totalCompleted(), 0);
    }

    @Test
    public void testCompleteAttemptWithIncorrectSession() {
        String queueName = UUID.randomUUID().toString();
        RaftWorkQueue queue1 = newPrimitive(queueName);
        byte[] item = DEFAULT_PAYLOAD;
        queue1.addOne(item).join();

        Task<byte[]> task = queue1.take().join();
        String taskId = task.taskId();

        // Create another client and get a handle to the same queue.
        RaftWorkQueue queue2 = newPrimitive(queueName);

        // Attempt completing the task with new client and verify task is not completed
        queue2.complete(taskId).join();

        WorkQueueStats stats = queue1.stats().join();
        assertEquals(stats.totalPending(), 0);
        assertEquals(stats.totalInProgress(), 1);
        assertEquals(stats.totalCompleted(), 0);
    }
}
