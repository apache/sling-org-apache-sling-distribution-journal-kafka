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

import static java.lang.String.format;
import static java.time.Duration.ofHours;
import static java.util.stream.Collectors.joining;
import static org.apache.sling.distribution.journal.RunnableUtil.startBackgroundThread;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

public class KafkaMessagePoller implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessagePoller.class);

    private final List<HandlerAdapter<?>> handlers;

    private final KafkaConsumer<String, byte[]> consumer;

    private volatile boolean running = true;

    private final String types;

    public KafkaMessagePoller(KafkaConsumer<String, byte[]> consumer, HandlerAdapter<?>... handlerAdapters) {
        this.consumer = consumer;
        handlers = Arrays.asList(handlerAdapters);
        types = handlers.stream().map(HandlerAdapter::getType).map(Class::getSimpleName).collect(joining(","));
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
        try {
            while(running) {
                consume();
            }
        } catch (WakeupException e) {
            if (running) {
                LOG.error("Waked up while running {}", e.getMessage(), e);
                throw e;
            } else {
                LOG.debug("Waked up while stopping {}", e.getMessage(), e);
            }
        } catch(Throwable t) {
            LOG.error(format("Catch Throwable %s closing consumer", t.getMessage()), t);
            throw t;
        } finally {
            consumer.close();
        }
        LOG.info("Stop poller for types {}", types);
    }

    private void consume() {
        consumer.poll(ofHours(1))
                .forEach(this::handleRecord);
    }

    private void handleRecord(ConsumerRecord<String, byte[]> record) {
        try {
            MessageInfo info = new KafkaMessageInfo(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp());
            ByteString payload = ByteString.copyFrom(record.value());
            Any any = Any.parseFrom(payload);
            Optional<HandlerAdapter<? extends Message>> handler = handlers.stream().filter(adapter -> any.is(adapter.getType())).findFirst();
            if (handler.isPresent()) {
                handler.get().handle(info, any);
            } else {
                LOG.debug("No handler found for {}", any);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

}
