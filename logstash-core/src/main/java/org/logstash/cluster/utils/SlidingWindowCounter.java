package org.logstash.cluster.utils;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.logstash.cluster.utils.concurrent.Scheduled;
import org.logstash.cluster.utils.concurrent.ThreadContext;

/**
 * Maintains a sliding window of value counts. The sliding window counter is
 * initialized with a number of window slots. Calls to #incrementCount() will
 * increment the value in the current window slot. Periodically the window
 * slides and the oldest value count is dropped. Calls to #get() will get the
 * total count for the last N window slots.
 */
public final class SlidingWindowCounter {
    private static final int SLIDE_WINDOW_PERIOD_SECONDS = 1;
    private final int windowSlots;

    private final List<AtomicLong> counters;

    private final Scheduled schedule;
    private volatile int headSlot;

    /**
     * Creates a new sliding window counter with the given total number of
     * window slots.
     * @param windowSlots total number of window slots
     */
    public SlidingWindowCounter(int windowSlots, ThreadContext context) {
        Preconditions.checkArgument(windowSlots > 0, "Window size must be a positive integer");

        this.windowSlots = windowSlots;
        this.headSlot = 0;

        // Initialize each item in the list to an AtomicLong of 0
        this.counters = Collections.nCopies(windowSlots, 0)
            .stream()
            .map(AtomicLong::new)
            .collect(Collectors.toCollection(ArrayList::new));
        this.schedule = context.schedule(0, SLIDE_WINDOW_PERIOD_SECONDS, TimeUnit.SECONDS, this::advanceHead);
    }

    /**
     * Releases resources used by the SlidingWindowCounter.
     */
    public void destroy() {
        schedule.cancel();
    }

    /**
     * Increments the count of the current window slot by 1.
     */
    public void incrementCount() {
        incrementCount(headSlot, 1);
    }

    private void incrementCount(int slot, long value) {
        counters.get(slot).addAndGet(value);
    }

    /**
     * Increments the count of the current window slot by the given value.
     * @param value value to increment by
     */
    public void incrementCount(long value) {
        incrementCount(headSlot, value);
    }

    /**
     * Gets the total count for the last N window slots.
     * @param slots number of slots to include in the count
     * @return total count for last N slots
     */
    public long get(int slots) {
        Preconditions.checkArgument(slots <= windowSlots,
            "Requested window must be less than the total window slots");

        long sum = 0;

        for (int i = 0; i < slots; i++) {
            int currentIndex = headSlot - i;
            if (currentIndex < 0) {
                currentIndex = counters.size() + currentIndex;
            }
            sum += counters.get(currentIndex).get();
        }

        return sum;
    }

    void advanceHead() {
        counters.get(slotAfter(headSlot)).set(0);
        headSlot = slotAfter(headSlot);
    }

    private int slotAfter(int slot) {
        return (slot + 1) % windowSlots;
    }

}
