/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.distribution.journal.kafka;

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;

import com.fasterxml.jackson.databind.ObjectReader;

public class JsonRecordHandler<T> {

    public final MessageHandler<T> handler;

    public final ObjectReader reader;

    public JsonRecordHandler(MessageHandler<T> handler, ObjectReader reader) {
        this.reader = reader;
        this.handler = requireNonNull(handler);
        
    }

    public void accept(ConsumerRecord<String, String> record) throws IOException {
        MessageInfo info = new KafkaMessageInfo(record);
        String payload = record.value();
        T message = reader.readValue(payload);
        handler.handle(info, message);
    }
}
