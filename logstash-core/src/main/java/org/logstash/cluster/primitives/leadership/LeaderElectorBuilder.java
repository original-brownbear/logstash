package org.logstash.cluster.primitives.leadership;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for constructing new {@link AsyncLeaderElector} instances.
 */
public abstract class LeaderElectorBuilder<T>
    extends DistributedPrimitiveBuilder<LeaderElectorBuilder<T>, LeaderElector<T>, AsyncLeaderElector<T>> {

    private Duration electionTimeout = Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);

    public LeaderElectorBuilder() {
        super(DistributedPrimitive.Type.LEADER_ELECTOR);
    }

    /**
     * Sets the election timeout in milliseconds.
     * @param electionTimeoutMillis the election timeout in milliseconds
     * @return leader elector builder
     */
    public LeaderElectorBuilder<T> withElectionTimeout(long electionTimeoutMillis) {
        return withElectionTimeout(Duration.ofMillis(electionTimeoutMillis));
    }

    /**
     * Sets the election timeout.
     * @param electionTimeout the election timeout
     * @return leader elector builder
     */
    public LeaderElectorBuilder<T> withElectionTimeout(Duration electionTimeout) {
        this.electionTimeout = Preconditions.checkNotNull(electionTimeout);
        return this;
    }

    /**
     * Sets the election timeout.
     * @param electionTimeout the election timeout
     * @param timeUnit the timeout time unit
     * @return leader elector builder
     */
    public LeaderElectorBuilder<T> withElectionTimeout(long electionTimeout, TimeUnit timeUnit) {
        return withElectionTimeout(Duration.ofMillis(timeUnit.toMillis(electionTimeout)));
    }

    /**
     * Returns the election timeout.
     * @return the election timeout
     */
    public Duration electionTimeout() {
        return electionTimeout;
    }

    @Override
    public LeaderElector<T> build() {
        return buildAsync().asLeaderElector();
    }
}
