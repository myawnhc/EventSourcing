/*
 * Copyright 2022 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package org.hazelcast.eventsourcing.pubsub;

/** Event sourcing needs a pub-sub mechanism to allow different microservices to be
 * notified of events of interest on services with which they interoperate.  This
 * interface represents the Consumer / Reader side of the pub-sub connection.
 *
 * @param <E> base class of events to be passed as messages
 */
public interface Consumer<E> {
    /** Callback implemented by consumers to receive message notifications */
    void onEvent(E eventMessage);
}
