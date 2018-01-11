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
package org.logstash.cluster.primitives.value.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.value.AsyncAtomicValue;
import org.logstash.cluster.primitives.value.AtomicValueBuilder;

/**
 * Default implementation of AtomicValueBuilder.
 * @param <V> value type
 */
public class DefaultAtomicValueBuilder<V> extends AtomicValueBuilder<V> {
    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultAtomicValueBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncAtomicValue<V> buildAsync() {
        return primitiveCreator.newAsyncAtomicValue(name(), serializer());
    }
}
