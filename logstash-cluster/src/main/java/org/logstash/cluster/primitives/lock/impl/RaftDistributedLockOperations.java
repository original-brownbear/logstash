/*
 * Copyright 2017-present Open Networking Foundation
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
package org.logstash.cluster.primitives.lock.impl;

import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Counter commands.
 */
public enum RaftDistributedLockOperations implements OperationId {
    LOCK("lock", OperationType.COMMAND),
    UNLOCK("unlock", OperationType.COMMAND);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(Lock.class)
        .register(Unlock.class)
        .build(RaftDistributedLockOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftDistributedLockOperations(String id, OperationType type) {
        this.id = id;
        this.type = type;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public OperationType type() {
        return type;
    }

    /**
     * Abstract lock operation.
     */
    public abstract static class LockOperation {
        @Override
        public String toString() {
            return toStringHelper(this).toString();
        }
    }

    /**
     * Lock command.
     */
    public static class Lock extends LockOperation {
        private final int id;
        private final long timeout;

        public Lock() {
            this(0, 0);
        }

        public Lock(int id, long timeout) {
            this.id = id;
            this.timeout = timeout;
        }

        /**
         * Returns the lock identifier.
         * @return the lock identifier
         */
        public int id() {
            return id;
        }

        /**
         * Returns the lock attempt timeout.
         * @return the lock attempt timeout
         */
        public long timeout() {
            return timeout;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("id", id)
                .add("timeout", timeout)
                .toString();
        }
    }

    /**
     * Unlock command.
     */
    public static class Unlock extends LockOperation {
        private final int id;

        public Unlock(int id) {
            this.id = id;
        }

        /**
         * Returns the lock identifier.
         * @return the lock identifier
         */
        public int id() {
            return id;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("id", id)
                .toString();
        }
    }
}
