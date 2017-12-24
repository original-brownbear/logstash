package org.logstash.cluster.primitives.queue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.logstash.cluster.primitives.AsyncPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.queue.impl.BlockingWorkQueue;

/**
 * Distributed Work Queue primitive.
 * <p>
 * Work queue serves as a buffer allowing producers to {@link #addMultiple(Collection) add} tasks and consumers
 * to {@link #take() take} tasks to process.
 * <p>
 * In the system each task is tracked via its unique task identifier which is returned when a task is taken.
 * Work queue guarantees that a task can be taken by only one consumer at a time. Once it finishes processing a
 * consumer must invoke the {@link #complete(Collection) complete} method to mark the task(s) as completed.
 * Tasks thus completed are removed from the queue. If a consumer unexpectedly terminates before it can complete
 * all its tasks are returned back to the queue so that other consumers can pick them up. Since there is a distinct
 * possibility that tasks could be processed more than once (under failure conditions), care should be taken to ensure
 * task processing logic is idempotent.
 * @param <E> task payload type.
 */
public interface AsyncWorkQueue<E> extends AsyncPrimitive {

    @Override
    default DistributedPrimitive.Type primitiveType() {
        return DistributedPrimitive.Type.WORK_QUEUE;
    }

    /**
     * Registers a task processing callback to be automatically invoked when new tasks are
     * added to the work queue.
     * @param taskProcessor task processing callback
     * @param parallelism max tasks that can be processed in parallel
     * @param executor executor to use for processing the tasks
     * @return future that is completed when the operation completes
     */
    CompletableFuture<Void> registerTaskProcessor(Consumer<E> taskProcessor,
        int parallelism,
        Executor executor);

    /**
     * Stops automatically processing tasks from work queue. This call nullifies the effect of a
     * previous {@link #registerTaskProcessor registerTaskProcessor} call.
     * @return future that is completed when the operation completes
     */
    CompletableFuture<Void> stopProcessing();

    /**
     * Returns work queue statistics.
     * @return future that is completed with work queue stats when the operation completes
     */
    CompletableFuture<WorkQueueStats> stats();

    /**
     * Completes a collection of tasks.
     * @param taskIds var arg list of task ids
     * @return future that is completed when the operation completes
     */
    default CompletableFuture<Void> complete(String... taskIds) {
        return complete(Arrays.asList(taskIds));
    }

    /**
     * Completes a collection of tasks.
     * @param taskIds ids of tasks to complete
     * @return future that is completed when the operation completes
     */
    CompletableFuture<Void> complete(Collection<String> taskIds);

    /**
     * Adds a single task to the work queue.
     * @param item task item
     * @return future that is completed when the operation completes
     */
    default CompletableFuture<Void> addOne(E item) {
        return addMultiple(ImmutableList.of(item));
    }

    /**
     * Adds a collection of tasks to the work queue.
     * @param items collection of task items
     * @return future that is completed when the operation completes
     */
    CompletableFuture<Void> addMultiple(Collection<E> items);

    /**
     * Picks up a single task from the work queue to work on.
     * <p>
     * Tasks that are taken remain invisible to other consumers as long as the consumer stays alive.
     * If a consumer unexpectedly terminates before {@link #complete(String...) completing} the task,
     * the task becomes visible again to other consumers to process.
     * @return future for the task. The future can be completed with null, if there are no
     * unassigned tasks in the work queue
     */
    default CompletableFuture<Task<E>> take() {
        return this.take(1).thenApply(tasks -> tasks.isEmpty() ? null : tasks.iterator().next());
    }

    /**
     * Picks up multiple tasks from the work queue to work on.
     * <p>
     * Tasks that are taken remain invisible to other consumers as long as the consumer stays alive.
     * If a consumer unexpectedly terminates before {@link #complete(String...) completing} the task,
     * the task becomes visible again to other consumers to process.
     * @param maxItems maximum number of items to take from the queue. The actual number of tasks returned
     * can be at the max this number
     * @return future for the tasks. The future can be completed with an empty collection if there are no
     * unassigned tasks in the work queue
     */
    CompletableFuture<Collection<Task<E>>> take(int maxItems);

    /**
     * Returns a synchronous {@link WorkQueue} instance that wraps this instance.
     * @return a synchronous {@link WorkQueue}. Changes to the returned queue will be reflected in this queue
     * and vice versa
     */
    default WorkQueue<E> asWorkQueue() {
        return asWorkQueue(DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }

    /**
     * Returns a synchronous {@link WorkQueue} instance that wraps this instance.
     * @param timeoutMillis number of milliseconds to block before timing out queue operations
     * @return a synchronous {@link WorkQueue}. Changes to the returned queue will be reflected in this queue
     * and vice versa
     */
    default WorkQueue<E> asWorkQueue(long timeoutMillis) {
        return new BlockingWorkQueue<>(this, timeoutMillis);
    }
}
