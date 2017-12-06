package org.logstash.cluster.utils.concurrent;

/**
 * Scheduled task.
 */
public interface Scheduled {

    /**
     * Cancels the scheduled task.
     */
    void cancel();

}
