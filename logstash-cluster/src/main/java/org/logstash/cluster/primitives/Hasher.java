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
package org.logstash.cluster.primitives;

/**
 * Interface for mapping from an object to partition ID.
 * @param <K> object type.
 */
public interface Hasher<K> {

    /**
     * Returns the partition ID to which the specified object maps.
     * @param object object
     * @return partition identifier
     */
    int hash(K object);

}
