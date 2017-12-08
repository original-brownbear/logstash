/*
 * Copyright 2016-present Open Networking Foundation
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
 * limitations under the License
 */
package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Member configuration change request.
 */
public class ReconfigureRequest extends ConfigurationRequest {

    private final long index;
    private final long term;
    public ReconfigureRequest(RaftMember member, long index, long term) {
        super(member);
        this.index = index;
        this.term = term;
    }

    /**
     * Returns a new reconfigure request builder.
     * @return A new reconfigure request builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the configuration index.
     * @return The configuration index.
     */
    public long index() {
        return index;
    }

    /**
     * Returns the configuration term.
     * @return The configuration term.
     */
    public long term() {
        return term;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), index, member);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof ReconfigureRequest) {
            ReconfigureRequest request = (ReconfigureRequest) object;
            return request.index == index && request.term == term && request.member.equals(member);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("index", index)
            .add("term", term)
            .add("member", member)
            .toString();
    }

    /**
     * Reconfigure request builder.
     */
    public static class Builder extends ConfigurationRequest.Builder<Builder, ReconfigureRequest> {
        private long index = -1;
        private long term = -1;

        /**
         * Sets the request index.
         * @param index The request index.
         * @return The request builder.
         */
        public Builder withIndex(long index) {
            Preconditions.checkArgument(index >= 0, "index must be positive");
            this.index = index;
            return this;
        }

        /**
         * Sets the request term.
         * @param term The request term.
         * @return The request builder.
         */
        public Builder withTerm(long term) {
            Preconditions.checkArgument(term >= 0, "term must be positive");
            this.term = term;
            return this;
        }

        @Override
        public ReconfigureRequest build() {
            validate();
            return new ReconfigureRequest(member, index, term);
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkArgument(index >= 0, "index must be positive");
            Preconditions.checkArgument(term >= 0, "term must be positive");
        }
    }
}
