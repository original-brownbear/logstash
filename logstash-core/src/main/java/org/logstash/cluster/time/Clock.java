package org.logstash.cluster.time;

/**
 * Clock.
 */
public interface Clock<T extends Timestamp> {

    /**
     * Returns the current time of the clock.
     * @return the current time
     */
    T getTime();

}
