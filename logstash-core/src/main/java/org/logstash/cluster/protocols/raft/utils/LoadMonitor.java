package org.logstash.cluster.protocols.raft.utils;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.utils.SlidingWindowCounter;
import org.logstash.cluster.utils.concurrent.ThreadContext;

/**
 * Server load monitor.
 */
public class LoadMonitor {
    private final SlidingWindowCounter loadCounter;
    private final int windowSize;
    private final int highLoadThreshold;

    public LoadMonitor(int windowSize, int highLoadThreshold, ThreadContext threadContext) {
        this.windowSize = windowSize;
        this.highLoadThreshold = highLoadThreshold;
        this.loadCounter = new SlidingWindowCounter(windowSize, threadContext);
    }

    /**
     * Records a load event.
     */
    public void recordEvent() {
        loadCounter.incrementCount();
    }

    /**
     * Returns a boolean indicating whether the server is under high load.
     * @return indicates whether the server is under high load
     */
    public boolean isUnderHighLoad() {
        return loadCounter.get(windowSize) > highLoadThreshold;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("windowSize", windowSize)
            .add("highLoadThreshold", highLoadThreshold)
            .toString();
    }
}
