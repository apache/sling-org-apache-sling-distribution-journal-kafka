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
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class JsonRecordHandler<T> implements Consumer<ConsumerRecord<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonRecordHandler.class);

    private final MessageHandler<T> handler;

    private final ObjectReader reader;

    public JsonRecordHandler(MessageHandler<T> handler, Class<T> clazz) {
        this.handler = requireNonNull(handler);
        ObjectMapper mapper = new ObjectMapper();
        reader = mapper.readerFor(requireNonNull(clazz));
    }

    @Override
    public void accept(ConsumerRecord<String, String> record) {
        MessageInfo info = new KafkaMessageInfo(record);
        String payload = record.value();
        try {
            T message = reader.readValue(payload);
            handler.handle(info, message);
        } catch (IOException e) {
            LOG.warn("Failed to parse payload {}", payload);
        }
    }
}
