package org.logstash.cluster.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base implementation of an item accumulator. It allows triggering based on
 * item inter-arrival time threshold, maximum batch life threshold and maximum
 * batch size.
 */
public abstract class AbstractAccumulator<T> implements Accumulator<T> {

    private static final Logger LOGGER = LogManager.getLogger(AbstractAccumulator.class);

    private final Timer timer;
    private final int maxItems;
    private final int maxBatchMillis;
    private final int maxIdleMillis;
    private final AtomicReference<TimerTask> idleTask = new AtomicReference<>();
    private final AtomicReference<TimerTask> maxTask = new AtomicReference<>();
    private final List<T> items;

    /**
     * Creates an item accumulator capable of triggering on the specified
     * thresholds.
     * @param timer timer to use for scheduling check-points
     * @param maxItems maximum number of items to accumulate before
     * processing is triggered
     * <p>
     * NB: It is possible that processItems will contain
     * more than maxItems under high load or if isReady()
     * can return false.
     * </p>
     * @param maxBatchMillis maximum number of millis allowed since the first
     * item before processing is triggered
     * @param maxIdleMillis maximum number millis between items before
     * processing is triggered
     */
    protected AbstractAccumulator(Timer timer, int maxItems,
        int maxBatchMillis, int maxIdleMillis) {
        this.timer = Preconditions.checkNotNull(timer, "Timer cannot be null");

        Preconditions.checkArgument(maxItems > 1, "Maximum number of items must be > 1");
        Preconditions.checkArgument(maxBatchMillis > 0, "Maximum millis must be positive");
        Preconditions.checkArgument(maxIdleMillis > 0, "Maximum idle millis must be positive");

        this.maxItems = maxItems;
        this.maxBatchMillis = maxBatchMillis;
        this.maxIdleMillis = maxIdleMillis;

        items = Lists.newArrayListWithExpectedSize(maxItems);
    }

    @Override
    public void add(T item) {
        final int sizeAtTimeOfAdd;
        synchronized (items) {
            items.add(item);
            sizeAtTimeOfAdd = items.size();
        }

        /*
            WARNING: It is possible that the item that was just added to the list
            has been processed by an existing idle task at this point.

            By rescheduling the following timers, it is possible that a
            superfluous maxTask is generated now OR that the idle task and max
            task are scheduled at their specified delays. This could result in
            calls to processItems sooner than expected.
         */

        // Did we hit the max item threshold?
        if (sizeAtTimeOfAdd >= maxItems) {
            if (maxIdleMillis < maxBatchMillis) {
                cancelTask(idleTask);
            }
            rescheduleTask(maxTask, 0 /* now! */);
        } else {
            // Otherwise, schedule idle task and if this is a first item
            // also schedule the max batch age task.
            if (maxIdleMillis < maxBatchMillis) {
                rescheduleTask(idleTask, maxIdleMillis);
            }
            if (sizeAtTimeOfAdd == 1) {
                rescheduleTask(maxTask, maxBatchMillis);
            }
        }
    }

    /**
     * Reschedules the specified task, cancelling existing one if applicable.
     * @param taskRef task reference
     * @param millis delay in milliseconds
     */
    private void rescheduleTask(AtomicReference<TimerTask> taskRef, long millis) {
        AbstractAccumulator.ProcessorTask newTask = new AbstractAccumulator.ProcessorTask();
        timer.schedule(newTask, millis);
        swapAndCancelTask(taskRef, newTask);
    }

    /**
     * Cancels the specified task if it has not run or is not running.
     * @param taskRef task reference
     */
    private static void cancelTask(AtomicReference<TimerTask> taskRef) {
        swapAndCancelTask(taskRef, null);
    }

    /**
     * Sets the new task and attempts to cancelTask the old one.
     * @param taskRef task reference
     * @param newTask new task
     */
    private static void swapAndCancelTask(AtomicReference<TimerTask> taskRef,
        TimerTask newTask) {
        TimerTask oldTask = taskRef.getAndSet(newTask);
        if (oldTask != null) {
            oldTask.cancel();
        }
    }

    @Override
    public boolean isReady() {
        return true;
    }

    /**
     * Returns an immutable copy of the existing items and clear the list.
     * @return list of existing items
     */
    private List<T> finalizeCurrentBatch() {
        List<T> finalizedList;
        synchronized (items) {
            finalizedList = ImmutableList.copyOf(items);
            items.clear();
            /*
             * To avoid reprocessing being triggered on an empty list.
             */
            cancelTask(maxTask);
            cancelTask(idleTask);
        }
        return finalizedList;
    }

    /**
     * Returns the backing timer.
     * @return backing timer
     */
    public Timer timer() {
        return timer;
    }

    /**
     * Returns the maximum number of items allowed to accumulate before
     * processing is triggered.
     * @return max number of items
     */
    public int maxItems() {
        return maxItems;
    }

    /**
     * Returns the maximum number of millis allowed to expire since the first
     * item before processing is triggered.
     * @return max number of millis a batch is allowed to last
     */
    public int maxBatchMillis() {
        return maxBatchMillis;
    }

    /**
     * Returns the maximum number of millis allowed to expire since the last
     * item arrival before processing is triggered.
     * @return max number of millis since the last item
     */
    public int maxIdleMillis() {
        return maxIdleMillis;
    }

    // Task for triggering processing of accumulated items
    private class ProcessorTask extends TimerTask {
        @Override
        public void run() {
            try {
                if (isReady()) {

                    List<T> batch = finalizeCurrentBatch();
                    if (!batch.isEmpty()) {
                        processItems(batch);
                    }
                } else {
                    rescheduleTask(idleTask, maxIdleMillis);
                }
            } catch (Exception e) {
                LOGGER.warn("Unable to process batch due to", e);
            }
        }
    }

}
