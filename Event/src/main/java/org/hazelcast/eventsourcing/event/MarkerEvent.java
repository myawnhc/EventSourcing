/*
 * Copyright 2024 Hazelcast, Inc
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

package org.hazelcast.eventsourcing.event;

public interface MarkerEvent<D extends DomainObject<String>>  {

    public String KEY = "key";

//    MarkerEventListener listener = null;
//    private List<Supplier<Boolean>> conditions;
//    private Runnable sucessAction;
//    private Runnable failureAction;

    void addListener(MarkerEventListener l);
    // Listener removal not needed

    void pipelineEntry();

    void pipelineExit();



//    static class Condition {
//        int retryCount;
//        Timer delay;
//        Supplier<Boolean> condition;
//
//        public boolean evaluate() {
//            // TODO: loop retry times with
//            return condition.get();
//        }
//    }
}
