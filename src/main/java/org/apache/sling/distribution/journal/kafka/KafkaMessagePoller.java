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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.sling.distribution.journal.messages.Types;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import com.google.protobuf.ByteString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofHours;
import static java.util.Objects.requireNonNull;

public class KafkaMessagePoller implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessagePoller.class);

    private final Map<Class<?>, HandlerAdapter<?>> handlers = new HashMap<>();

    private final KafkaConsumer<String, byte[]> consumer;

    private volatile boolean running = true;

    private final String types;

    private final ExceptionEventSender eventSender;

    public KafkaMessagePoller(KafkaConsumer<String, byte[]> consumer, ExceptionEventSender eventSender, HandlerAdapter<?>... handlerAdapters) {
        this.consumer = requireNonNull(consumer);
        this.eventSender = requireNonNull(eventSender);
        for (HandlerAdapter<?> handlerAdapter : handlerAdapters) {
            handlers.put(handlerAdapter.getType(), handlerAdapter);
        }
        types = handlers.keySet().toString();
        startBackgroundThread(this::run, format("Message Poller %s", types));
    }

    @Override
    public void close() throws IOException {
        LOG.info("Shutdown poller for types {}", types);
        running = false;
        consumer.wakeup();
    }

    public void run() {
        LOG.info("Start poller for types {}", types);
        while(running) {
            try {
                consumer.poll(ofHours(1))
                .forEach(this::handleRecord);
            } catch (WakeupException e) {
                LOG.debug("Waked up {}", e.getMessage(), e);
                this.running = false;
            } catch(Exception e) {
                eventSender.send(e);
                LOG.error("Exception while receiving from kafka: {}", e.getMessage(), e);
                sleepAfterError();
                // Continue as KafkaConsumer should handle the error transparently
            }
        }
        consumer.close();
        LOG.info("Stop poller for types {}", types);
    }

    private void sleepAfterError() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleRecord(ConsumerRecord<String, byte[]> record) {
        getHandler(record)
            .ifPresent(handler->handleRecord(handler, record));
    }

    private Optional<HandlerAdapter<?>> getHandler(ConsumerRecord<String, byte[]> record) {
        try {
            int type = parseInt(getHeaderValue(record.headers(), "type"));
            int version = parseInt(getHeaderValue(record.headers(), "version"));
            Class<?> messageClass = Types.getType(type, version);
            Optional<HandlerAdapter<?>> handler = Optional.ofNullable(handlers.get(messageClass));
            if (!handler.isPresent()) {
                LOG.debug("No handler registered for type {}", messageClass.getName());
            }
            return handler;
        } catch (RuntimeException e) {
            LOG.info("No handler found for headers {}.", record.headers(), e);
            return Optional.empty();
        }
    }

    private void handleRecord(HandlerAdapter<?> handler, ConsumerRecord<String, byte[]> record) {
        try {
            MessageInfo info = new KafkaMessageInfo(record);
            ByteString payload = ByteString.copyFrom(record.value());
            handler.handle(info, payload);
        } catch (Exception e) {
            String msg = format("Error consuming message for types %s", types);
            LOG.warn(msg);
        }
    }

    private String getHeaderValue(Headers headers, String key) {
        Header header = Optional.ofNullable(headers.lastHeader(key))
            .orElseThrow(()->new IllegalArgumentException(format("Header with key %s not found", key)));
        return new String(header.value(), UTF_8);
    }
}
