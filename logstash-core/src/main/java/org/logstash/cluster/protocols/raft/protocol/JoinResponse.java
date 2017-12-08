/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.Preconditions;
import java.util.Collection;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Server join configuration change response.
 */
public class JoinResponse extends ConfigurationResponse {

    public JoinResponse(Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
        super(status, error, index, term, timestamp, members);
    }

    /**
     * Returns a new join response builder.
     * @return A new join response builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Join response builder.
     */
    public static class Builder extends ConfigurationResponse.Builder<Builder, JoinResponse> {
        @Override
        public JoinResponse build() {
            validate();
            return new JoinResponse(status, error, index, term, timestamp, members);
        }

        @Override
        protected void validate() {
            // JoinResponse allows null errors indicating the client should retry.
            Preconditions.checkNotNull(status, "status cannot be null");
            if (status == Status.OK) {
                Preconditions.checkArgument(index >= 0, "index must be positive");
                Preconditions.checkArgument(term >= 0, "term must be positive");
                Preconditions.checkArgument(timestamp > 0, "time must be positive");
                Preconditions.checkNotNull(members, "members cannot be null");
            }
        }
    }
}
