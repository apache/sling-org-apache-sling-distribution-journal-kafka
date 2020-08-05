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

import static java.time.Duration.ofHours;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class KafkaPoller implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPoller.class);

    private static final long ERROR_SLEEP_MS = 10000;

    private final KafkaConsumer<String, String> consumer;

    private final Map<String, JsonRecordHandler<?>> handlers;
    
    private final ExceptionEventSender eventSender;
    
    private final ObjectMapper mapper;

    private volatile boolean running = true;

    long errorSleepMs;

    public KafkaPoller(KafkaConsumer<String, String> consumer, ExceptionEventSender eventSender, List<HandlerAdapter<?>> adapters) {
        this.consumer = requireNonNull(consumer);
        this.eventSender = requireNonNull(eventSender);
        this.errorSleepMs = ERROR_SLEEP_MS;
        mapper = new ObjectMapper();
        this.handlers = adapters.stream().collect(toMap(this::typeName, this::toHandler));
        startBackgroundThread(this::run, "Message Poller");
    }

    private String typeName(HandlerAdapter<?> adapter) {
        return adapter.getType().getSimpleName();
    }
    
    <T> JsonRecordHandler<T> toHandler(HandlerAdapter<T> adapter) {
        ObjectReader reader = mapper.readerFor(adapter.getType());
        return new JsonRecordHandler<>(adapter.getHandler(), reader);
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Shutdown poller");
        running = false;
        consumer.wakeup();
    }

    public void run() {
        LOG.info("Start poller");
        while(running) {
            try {
                consumer.poll(ofHours(1)).forEach(this::handle);
            } catch (WakeupException e) {
                LOG.debug("Waked up {}", e.getMessage(), e);
                this.running = false;
            } catch (Exception e) {
                eventSender.send(e);
                LOG.error("Exception while receiving from kafka: {}", e.getMessage(), e);
                sleepAfterError();
                // Continue as KafkaConsumer should handle the error transparently
            }
        }
        consumer.close();
        LOG.info("Stopped poller");
    }
    
    public void handle(ConsumerRecord<String, String> record) {
        try {
            String messageType = getMessageType(record);
            JsonRecordHandler<?> handler = handlers.get(messageType);
            if (handler != null) {
                handler.accept(record);
            } else {
                LOG.info("No handler for messageType={}. Ignoring message.", messageType);
            }
        } catch (Exception e) {
            LOG.warn("Error consuming message {}", record.headers(), e);
        }
    }

    private String getMessageType(ConsumerRecord<String, String> record) {
        Iterator<Header> headers = record.headers().headers(KafkaMessageInfo.KEY_MESSAGE_TYPE).iterator();
        if (!headers.hasNext()) {
            throw new MessagingException("Header " + KafkaMessageInfo.KEY_MESSAGE_TYPE + " missing.");
        }
        Header messageTypeHeader = headers.next();
        return new String(messageTypeHeader.value(), StandardCharsets.UTF_8);
    }

    private void sleepAfterError() {
        try {
            Thread.sleep(errorSleepMs);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
        }
    }

}
