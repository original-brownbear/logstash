package org.logstash.cluster.utils.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Ordered completable future test.
 */
public class OrderedFutureTest {

    /**
     * Tests ordered completion of future callbacks.
     */
    @Test
    public void testOrderedCompletion() {
        CompletableFuture<String> future = new OrderedFuture<>();
        AtomicInteger order = new AtomicInteger();
        future.whenComplete((r, e) -> assertEquals(1, order.incrementAndGet()));
        future.whenComplete((r, e) -> assertEquals(2, order.incrementAndGet()));
        future.handle((r, e) -> {
            assertEquals(3, order.incrementAndGet());
            assertEquals(r, "foo");
            return "bar";
        });
        future.thenRun(() -> assertEquals(3, order.incrementAndGet()));
        future.thenAccept(r -> {
            assertEquals(5, order.incrementAndGet());
            assertEquals(r, "foo");
        });
        future.thenApply(r -> {
            assertEquals(6, order.incrementAndGet());
            assertEquals(r, "foo");
            return "bar";
        });
        future.whenComplete((r, e) -> {
            assertEquals(7, order.incrementAndGet());
            assertEquals(r, "foo");
        });
        future.complete("foo");
    }

    /**
     * Tests ordered failure of future callbacks.
     */
    @Test
    public void testOrderedFailure() {
        CompletableFuture<String> future = new OrderedFuture<>();
        AtomicInteger order = new AtomicInteger();
        future.whenComplete((r, e) -> assertEquals(1, order.incrementAndGet()));
        future.whenComplete((r, e) -> assertEquals(2, order.incrementAndGet()));
        future.handle((r, e) -> {
            assertEquals(3, order.incrementAndGet());
            return "bar";
        });
        future.thenRun(() -> fail());
        future.thenAccept(r -> fail());
        future.exceptionally(e -> {
            assertEquals(3, order.incrementAndGet());
            return "bar";
        });
        future.completeExceptionally(new RuntimeException("foo"));
    }

    /**
     * Tests calling callbacks that are added after completion.
     */
    @Test
    public void testAfterComplete() {
        CompletableFuture<String> future = new OrderedFuture<>();
        future.whenComplete((result, error) -> assertEquals(result, "foo"));
        future.complete("foo");
        AtomicInteger count = new AtomicInteger();
        future.whenComplete((result, error) -> {
            assertEquals(result, "foo");
            assertEquals(count.incrementAndGet(), 1);
        });
        future.thenAccept(result -> {
            assertEquals(result, "foo");
            assertEquals(count.incrementAndGet(), 2);
        });
        assertEquals(count.get(), 2);
    }
}
