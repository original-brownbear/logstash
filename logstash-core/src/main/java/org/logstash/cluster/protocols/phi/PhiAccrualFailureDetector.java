package org.logstash.cluster.protocols.phi;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Phi Accrual failure detector.
 * <p>
 * Based on a paper titled: "The φ Accrual Failure Detector" by Hayashibara, et al.
 */
public class PhiAccrualFailureDetector {

    // Default value
    private static final int DEFAULT_WINDOW_SIZE = 250;
    private static final int DEFAULT_MIN_SAMPLES = 25;
    private static final double DEFAULT_PHI_FACTOR = 1.0 / Math.log(10.0);
    private final int minSamples;
    private final double phiFactor;
    private final PhiAccrualFailureDetector.History history;

    /**
     * Creates a new failure detector with the default configuration.
     */
    public PhiAccrualFailureDetector() {
        this(DEFAULT_MIN_SAMPLES, DEFAULT_PHI_FACTOR, DEFAULT_WINDOW_SIZE);
    }

    /**
     * Creates a new failure detector.
     * @param minSamples the minimum number of samples required to compute phi
     * @param phiFactor the phi factor
     * @param windowSize the phi accrual window size
     */
    public PhiAccrualFailureDetector(int minSamples, double phiFactor, int windowSize) {
        this.minSamples = minSamples;
        this.phiFactor = phiFactor;
        this.history = new PhiAccrualFailureDetector.History(windowSize);
    }

    /**
     * Report a new heart beat for the specified node id.
     */
    public void report() {
        report(System.currentTimeMillis());
    }

    /**
     * Report a new heart beat for the specified node id.
     * @param arrivalTime arrival time
     */
    public void report(long arrivalTime) {
        Preconditions.checkArgument(arrivalTime >= 0, "arrivalTime must not be negative");
        long latestHeartbeat = history.latestHeartbeatTime();
        if (latestHeartbeat != -1) {
            history.samples().addValue(arrivalTime - latestHeartbeat);
        }
        history.setLatestHeartbeatTime(arrivalTime);
    }

    /**
     * Compute phi for the specified node id.
     * @return phi value
     */
    public double phi() {
        long latestHeartbeat = history.latestHeartbeatTime();
        DescriptiveStatistics samples = history.samples();
        if (latestHeartbeat == -1 || samples.getN() < minSamples) {
            return 0.0;
        }
        return computePhi(samples, latestHeartbeat, System.currentTimeMillis());
    }

    /**
     * Computes the phi value from the given samples.
     * @param samples the samples from which to compute phi
     * @param lastHeartbeat the last heartbeat
     * @param currentTime the current time
     * @return phi
     */
    private double computePhi(DescriptiveStatistics samples, long lastHeartbeat, long currentTime) {
        long size = samples.getN();
        long t = currentTime - lastHeartbeat;
        return (size > 0)
            ? phiFactor * t / samples.getMean()
            : 100;
    }

    /**
     * Stores the history of heartbeats for a node.
     */
    private static class History {
        private final DescriptiveStatistics samples;
        long lastHeartbeatTime = -1;

        private History(int windowSize) {
            this.samples = new DescriptiveStatistics(windowSize);
        }

        DescriptiveStatistics samples() {
            return samples;
        }

        long latestHeartbeatTime() {
            return lastHeartbeatTime;
        }

        void setLatestHeartbeatTime(long value) {
            lastHeartbeatTime = value;
        }
    }
}
