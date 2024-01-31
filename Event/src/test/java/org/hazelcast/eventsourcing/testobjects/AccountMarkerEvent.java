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

package org.hazelcast.eventsourcing.testobjects;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.eventsourcing.event.MarkerEvent;
import org.hazelcast.eventsourcing.event.MarkerEventListener;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public class AccountMarkerEvent extends AccountEvent
                                implements MarkerEvent<Account> {

    private MarkerEventListener listener = null;

    public static final String QUAL_EVENT_NAME = "AccountService.AccountMarkerEvent";
    public static final String LISTENER = "listener";

    public AccountMarkerEvent(String key) {
        this.eventName = QUAL_EVENT_NAME;
        this.key = key;
    }

    public AccountMarkerEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.key = data.getString(KEY);
        String eventListener = data.getString(LISTENER);
        byte[] bytes = Base64.getDecoder().decode(eventListener.getBytes());
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            this.listener = (MarkerEventListener) ois.readObject();
            ois.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /* Used during 'materialize' but not normally during pipeline execution */
    public AccountMarkerEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        this.key = row.getObject("doKey"); // don't recall why named differently than GenericRecord key
    }

    @Override
    public void addListener(MarkerEventListener l) {
        listener = l;
    }

    @Override
    public void pipelineEntry() {
        if (listener != null)
            listener.notify("Marker Event reached Pipeline entry");
    }

    @Override
    public void pipelineExit() {
        if (listener != null);
            listener.notify("Marker Event reached Pipeline exit");
    }

    @Override
    public GenericRecord toGenericRecord() {
        String eventListener = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(listener);
            oos.close();
            eventListener = Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
        GenericRecord ame = GenericRecordBuilder.compact(QUAL_EVENT_NAME)
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setString(KEY, key)
                .setString(LISTENER, eventListener)
                .build();
        return ame;
    }

    @Override
    public GenericRecord apply(GenericRecord genericRecord) {
        // expect this will generally be a nop - return entry unchanged
        return genericRecord;
    }
}
