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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.cluster.MemberId;

/**
 * Leadership transfer request.
 */
public class TransferRequest extends AbstractRaftRequest {

    protected final MemberId member;

    protected TransferRequest(MemberId member) {
        this.member = member;
    }

    /**
     * Returns a new transfer request builder.
     * @return A new transfer request builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the member to which to transfer.
     * @return The member to which to transfer.
     */
    public MemberId member() {
        return member;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), member);
    }

    @Override
    public boolean equals(Object object) {
        if (getClass().isAssignableFrom(object.getClass())) {
            return ((ConfigurationRequest) object).member.equals(member);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("member", member)
            .toString();
    }

    /**
     * Transfer request builder.
     */
    public static class Builder extends AbstractRaftRequest.Builder<Builder, TransferRequest> {
        protected MemberId member;

        /**
         * Sets the request member.
         * @param member The request member.
         * @return The request builder.
         * @throws NullPointerException if {@code member} is null
         */
        @SuppressWarnings("unchecked")
        public Builder withMember(MemberId member) {
            this.member = Preconditions.checkNotNull(member, "member cannot be null");
            return this;
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkNotNull(member, "member cannot be null");
        }

        @Override
        public TransferRequest build() {
            return new TransferRequest(member);
        }
    }
}
