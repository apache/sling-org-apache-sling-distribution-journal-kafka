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
import static java.util.Objects.requireNonNull;
import static org.apache.sling.distribution.journal.kafka.KafkaClientProvider.PARTITION;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class KafkaJsonMessageSender<T> implements MessageSender<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonMessageSender.class);

    private final ObjectMapper mapper = new ObjectMapper();

    private final KafkaProducer<String, String> producer;

    private final ExceptionEventSender eventSender;

    private final String topic;

    public KafkaJsonMessageSender(KafkaProducer<String, String> producer, String topic, ExceptionEventSender eventSender) {
        this.topic = topic;
        this.eventSender = eventSender;
        this.producer = requireNonNull(producer);
    }

    
    @Override
    public void send(T payload) throws MessagingException {
        send(payload, Collections.emptyMap());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void send(T payload, Map<String, String> properties) throws MessagingException {
        try {
            ObjectWriter writer = mapper.writerFor(payload.getClass());
            String payloadSt = writer.writeValueAsString(payload);
            List<Header> headerList = properties.entrySet().stream().map(this::toHeader).collect(Collectors.toList());
            RecordHeader messageType = header(KafkaMessageInfo.KEY_MESSAGE_TYPE, payload.getClass().getSimpleName());
            headerList.add(messageType);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, PARTITION, null, payloadSt, headerList);
            RecordMetadata metadata = producer.send(record).get();
            LOG.info("Sent to topic={}, offset={}", topic, metadata.offset());
        } catch (Exception e) {
            eventSender.send(e);
            throw new MessagingException(format("Failed to send JSON message on topic %s", topic), e);
        }
    }

    private Header toHeader(Entry<String, String> entry) {
        return header(entry.getKey(), entry.getValue());
    }


    private RecordHeader header(String key, String value) {
        return new RecordHeader(key, value.getBytes(Charset.forName("utf-8")));
    }
}
