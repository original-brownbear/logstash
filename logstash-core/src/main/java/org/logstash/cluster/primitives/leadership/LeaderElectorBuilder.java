/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logstash.cluster.primitives.leadership;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

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
        this.electionTimeout = checkNotNull(electionTimeout);
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
