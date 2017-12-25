package org.logstash.cluster.protocols.raft.proxy;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.logstash.cluster.protocols.raft.RaftException;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.utils.Managed;

/**
 * Raft proxy client.
 */
public interface RaftProxyClient extends RaftProxyExecutor, Managed<RaftProxyClient> {

    /**
     * Raft session builder.
     */
    abstract class Builder implements org.logstash.cluster.utils.Builder<RaftProxyClient> {
        protected String name;
        protected ServiceType serviceType;
        protected ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
        protected int maxRetries = 0;
        protected Duration retryDelay = Duration.ofMillis(100);
        protected Executor executor;
        protected CommunicationStrategy communicationStrategy = CommunicationStrategy.LEADER;
        protected RecoveryStrategy recoveryStrategy = RecoveryStrategy.RECOVER;
        protected Duration minTimeout = Duration.ofMillis(250);
        protected Duration maxTimeout = Duration.ofMillis(0);

        /**
         * Sets the session name.
         * @param name The service name.
         * @return The session builder.
         */
        public Builder withName(String name) {
            this.name = Preconditions.checkNotNull(name, "name cannot be null");
            return this;
        }

        /**
         * Sets the service type.
         * @param serviceType The service type.
         * @return The session builder.
         */
        public Builder withServiceType(String serviceType) {
            return withServiceType(ServiceType.from(serviceType));
        }

        /**
         * Sets the service type.
         * @param serviceType The service type.
         * @return The session builder.
         */
        public Builder withServiceType(ServiceType serviceType) {
            this.serviceType = Preconditions.checkNotNull(serviceType, "serviceType cannot be null");
            return this;
        }

        /**
         * Sets the session's read consistency level.
         * @param consistency the session's read consistency level
         * @return the proxy builder
         */
        public Builder withReadConsistency(ReadConsistency consistency) {
            this.readConsistency = Preconditions.checkNotNull(consistency, "consistency cannot be null");
            return this;
        }

        /**
         * Sets the session's communication strategy.
         * @param communicationStrategy The session's communication strategy.
         * @return The session builder.
         * @throws NullPointerException if the communication strategy is null
         */
        public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
            this.communicationStrategy = Preconditions.checkNotNull(communicationStrategy, "communicationStrategy");
            return this;
        }

        /**
         * Sets the maximum number of retries before an operation can be failed.
         * @param maxRetries the maximum number of retries before an operation can be failed
         * @return the proxy builder
         */
        public Builder withMaxRetries(int maxRetries) {
            Preconditions.checkArgument(maxRetries >= 0, "maxRetries must be positive");
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the operation retry delay.
         * @param retryDelayMillis the delay between operation retries in milliseconds
         * @return the proxy builder
         */
        public Builder withRetryDelayMillis(long retryDelayMillis) {
            return withRetryDelay(Duration.ofMillis(retryDelayMillis));
        }

        /**
         * Sets the operation retry delay.
         * @param retryDelay the delay between operation retries
         * @return the proxy builder
         * @throws NullPointerException if the delay is null
         */
        public Builder withRetryDelay(Duration retryDelay) {
            this.retryDelay = Preconditions.checkNotNull(retryDelay, "retryDelay cannot be null");
            return this;
        }

        /**
         * Sets the operation retry delay.
         * @param retryDelay the delay between operation retries
         * @param timeUnit the delay time unit
         * @return the proxy builder
         * @throws NullPointerException if the time unit is null
         */
        public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
            return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
        }

        /**
         * Sets the session recovery strategy.
         * @param recoveryStrategy the session recovery strategy
         * @return the proxy builder
         * @throws NullPointerException if the strategy is null
         */
        public Builder withRecoveryStrategy(RecoveryStrategy recoveryStrategy) {
            this.recoveryStrategy = Preconditions.checkNotNull(recoveryStrategy, "recoveryStrategy cannot be null");
            return this;
        }

        /**
         * Sets the session timeout.
         * @param timeoutMillis The session timeout.
         * @return The session builder.
         * @throws IllegalArgumentException if the session timeout is not positive
         */
        public Builder withMinTimeout(long timeoutMillis) {
            return withMinTimeout(Duration.ofMillis(timeoutMillis));
        }

        /**
         * Sets the session timeout.
         * @param timeout The session timeout.
         * @return The session builder.
         * @throws IllegalArgumentException if the session timeout is not positive
         * @throws NullPointerException if the timeout is null
         */
        public Builder withMinTimeout(Duration timeout) {
            Preconditions.checkArgument(!Preconditions.checkNotNull(timeout).isNegative(), "timeout must be positive");
            this.minTimeout = timeout;
            return this;
        }

        /**
         * Sets the session timeout.
         * @param timeout The session timeout.
         * @return The session builder.
         * @throws IllegalArgumentException if the session timeout is not positive
         * @throws NullPointerException if the timeout is null
         */
        public Builder withMaxTimeout(Duration timeout) {
            Preconditions.checkArgument(!Preconditions.checkNotNull(timeout).isNegative(), "timeout must be positive");
            this.maxTimeout = timeout;
            return this;
        }

        /**
         * Sets the session timeout.
         * @param timeoutMillis The session timeout.
         * @return The session builder.
         * @throws IllegalArgumentException if the session timeout is not positive
         */
        public Builder withMaxTimeout(long timeoutMillis) {
            return withMaxTimeout(Duration.ofMillis(timeoutMillis));
        }

        /**
         * Builds the proxy client.
         * @return the proxy client
         */
        @Override
        public RaftProxyClient build() {
            try {
                return buildAsync().join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof RaftException.Unavailable) {
                    throw (RaftException.Unavailable) e.getCause();
                } else {
                    throw new RaftException.Unavailable(e);
                }
            }
        }

        /**
         * Returns a future to be completed once the proxy client has been connected.
         * @return a future to be completed once the proxy client has been connected
         */
        public abstract CompletableFuture<RaftProxyClient> buildAsync();
    }
}
