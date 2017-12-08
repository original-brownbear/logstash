/*
 * Copyright 2017-present Open Networking Foundation
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
package org.logstash.cluster.protocols.phi;

import org.logstash.cluster.event.EventListener;
import org.logstash.cluster.utils.Identifier;

/**
 * Failure detection event listener.
 */
public interface FailureDetectionEventListener<T extends Identifier> extends EventListener<FailureDetectionEvent<T>> {
}