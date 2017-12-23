package org.logstash.cluster.protocols.raft.storage.log.entry;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * Open session entry.
 */
public class OpenSessionEntry extends TimestampedEntry {
    private final String memberId;
    private final String serviceName;
    private final String serviceType;
    private final ReadConsistency readConsistency;
    private final long minTimeout;
    private final long maxTimeout;

    public OpenSessionEntry(long term, long timestamp, String memberId, String serviceName, String serviceType, ReadConsistency readConsistency, long minTimeout, long maxTimeout) {
        super(term, timestamp);
        this.memberId = memberId;
        this.serviceName = serviceName;
        this.serviceType = serviceType;
        this.readConsistency = readConsistency;
        this.minTimeout = minTimeout;
        this.maxTimeout = maxTimeout;
    }

    /**
     * Returns the client node identifier.
     * @return The client node identifier.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * Returns the session state machine name.
     * @return The session's state machine name.
     */
    public String serviceName() {
        return serviceName;
    }

    /**
     * Returns the session state machine type name.
     * @return The session's state machine type name.
     */
    public String serviceType() {
        return serviceType;
    }

    /**
     * Returns the session read consistency level.
     * @return The session's read consistency level.
     */
    public ReadConsistency readConsistency() {
        return readConsistency;
    }

    /**
     * Returns the minimum session timeout.
     * @return The minimum session timeout.
     */
    public long minTimeout() {
        return minTimeout;
    }

    /**
     * Returns the maximum session timeout.
     * @return The maximum session timeout.
     */
    public long maxTimeout() {
        return maxTimeout;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("timestamp", new TimestampPrinter(timestamp))
            .add("node", memberId)
            .add("serviceName", serviceName)
            .add("serviceType", serviceType)
            .add("readConsistency", readConsistency)
            .add("minTimeout", minTimeout)
            .add("maxTimeout", maxTimeout)
            .toString();
    }
}
