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
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPoller<T> implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPoller.class);

    private static final long ERROR_SLEEP_MS = 10000;

    private final KafkaConsumer<String, T> consumer;

    private final Consumer<ConsumerRecord<String, T>> handler;
    
    private final ExceptionEventSender eventSender;
    
    private volatile boolean running = true;

    long errorSleepMs;

    public KafkaPoller(KafkaConsumer<String, T> consumer, ExceptionEventSender eventSender, Consumer<ConsumerRecord<String, T>> handler) {
        this.handler = handler;
        this.consumer = requireNonNull(consumer);
        this.eventSender = requireNonNull(eventSender);
        this.errorSleepMs = ERROR_SLEEP_MS;
        startBackgroundThread(this::run, "Message Poller");
    }
    
    public static Closeable createProtobufPoller(KafkaConsumer<String, byte[]> consumer, ExceptionEventSender eventSender, HandlerAdapter<?>... adapters) {
        Consumer<ConsumerRecord<String, byte[]>> handler = new ProtobufRecordHandler(adapters);
        return new KafkaPoller<byte[]>(consumer, eventSender, handler);
    }
    
    public static <T>  Closeable createJsonPoller(KafkaConsumer<String, String> consumer, ExceptionEventSender eventSender, MessageHandler<T> handler, Class<T> clazz) {
        Consumer<ConsumerRecord<String, String>> recordHandler = new JsonRecordHandler<T>(handler, clazz);
        return new KafkaPoller<String>(consumer, eventSender, recordHandler);
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
    
    public void handle(ConsumerRecord<String, T> record) {
        try {
            handler.accept(record);
        } catch (Exception e) {
            LOG.warn("Error consuming message {}", record.headers());
        }
    }

    private void sleepAfterError() {
        try {
            Thread.sleep(errorSleepMs);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
        }
    }

}
