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

package org.logstash.cluster.primitives.tree.impl;

import com.google.common.base.MoreObjects;

/**
 * Result of a document tree operation.
 * @param <V> value type
 */
public class DocumentTreeResult<V> {

    @SuppressWarnings("unchecked")
    public static final DocumentTreeResult WRITE_LOCK =
        new DocumentTreeResult(DocumentTreeResult.Status.WRITE_LOCK, null);
    @SuppressWarnings("unchecked")
    public static final DocumentTreeResult INVALID_PATH =
        new DocumentTreeResult(DocumentTreeResult.Status.INVALID_PATH, null);
    @SuppressWarnings("unchecked")
    public static final DocumentTreeResult ILLEGAL_MODIFICATION =
        new DocumentTreeResult(DocumentTreeResult.Status.ILLEGAL_MODIFICATION, null);
    private final DocumentTreeResult.Status status;
    private final V result;

    public DocumentTreeResult(DocumentTreeResult.Status status, V result) {
        this.status = status;
        this.result = result;
    }

    /**
     * Returns a successful result.
     * @param result the operation result
     * @param <V> the result value type
     * @return successful result
     */
    public static <V> DocumentTreeResult<V> ok(V result) {
        return new DocumentTreeResult<>(DocumentTreeResult.Status.OK, result);
    }

    /**
     * Returns a {@code WRITE_LOCK} error result.
     * @param <V> the result value type
     * @return write lock result
     */
    @SuppressWarnings("unchecked")
    public static <V> DocumentTreeResult<V> writeLock() {
        return WRITE_LOCK;
    }

    /**
     * Returns an {@code INVALID_PATH} result.
     * @param <V> the result value type
     * @return invalid path result
     */
    @SuppressWarnings("unchecked")
    public static <V> DocumentTreeResult<V> invalidPath() {
        return INVALID_PATH;
    }

    /**
     * Returns an {@code ILLEGAL_MODIFICATION} result.
     * @param <V> the result value type
     * @return illegal modification result
     */
    @SuppressWarnings("unchecked")
    public static <V> DocumentTreeResult<V> illegalModification() {
        return ILLEGAL_MODIFICATION;
    }

    public DocumentTreeResult.Status status() {
        return status;
    }

    public V result() {
        return result;
    }

    public boolean created() {
        return updated() && result == null;
    }

    public boolean updated() {
        return status == DocumentTreeResult.Status.OK;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
            .add("status", status)
            .add("value", result)
            .toString();
    }

    public enum Status {
        /**
         * Indicates a successful update.
         */
        OK,

        /**
         * Indicates a noop i.e. existing and new value are both same.
         */
        NOOP,

        /**
         * Indicates a failed update due to a write lock.
         */
        WRITE_LOCK,

        /**
         * Indicates a failed update due to a invalid path.
         */
        INVALID_PATH,

        /**
         * Indicates a failed update due to a illegal modification attempt.
         */
        ILLEGAL_MODIFICATION,
    }
}
