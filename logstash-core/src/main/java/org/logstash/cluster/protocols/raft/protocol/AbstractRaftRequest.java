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

/**
 * Base request for all client requests.
 */
public abstract class AbstractRaftRequest implements RaftRequest {

    /**
     * Abstract request builder.
     * @param <T> The builder type.
     * @param <U> The request type.
     */
    protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractRaftRequest> implements RaftRequest.Builder<T, U> {

        /**
         * Validates the builder.
         */
        protected void validate() {
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).toString();
        }
    }
}
