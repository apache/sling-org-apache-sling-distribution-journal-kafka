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

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.messages.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class ProtobufRecordHandler implements Consumer<ConsumerRecord<String, byte[]>> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufRecordHandler.class);

    private final Map<Class<?>, HandlerAdapter<?>> handlers = new HashMap<>();

    public ProtobufRecordHandler(HandlerAdapter<?>... handlerAdapters) {
        for (HandlerAdapter<?> handlerAdapter : handlerAdapters) {
            handlers.put(handlerAdapter.getType(), handlerAdapter);
        }
    }

    @Override
    public void accept(ConsumerRecord<String, byte[]> record) {
        getHandler(record)
            .ifPresent(handler->handleRecord(handler, record));
    }

    private Optional<HandlerAdapter<?>> getHandler(ConsumerRecord<String, byte[]> record) {
        int type = parseInt(getHeaderValue(record.headers(), "type"));
        int version = parseInt(getHeaderValue(record.headers(), "version"));
        Class<?> messageClass = Types.getType(type, version);
        Optional<HandlerAdapter<?>> handler = Optional.ofNullable(handlers.get(messageClass));
        if (!handler.isPresent()) {
            LOG.debug("No handler registered for type {}", messageClass.getName());
        }
        return handler;
    }

    private void handleRecord(HandlerAdapter<?> handler, ConsumerRecord<String, byte[]> record) {
        MessageInfo info = new KafkaMessageInfo(record);
        ByteString payload = ByteString.copyFrom(record.value());
        handler.handle(info, payload);
    }

    private String getHeaderValue(Headers headers, String key) {
        Header header = Optional.ofNullable(headers.lastHeader(key))
            .orElseThrow(()->new IllegalArgumentException(format("Header with key %s not found", key)));
        return new String(header.value(), UTF_8);
    }
}
