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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.MessageInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(MockitoJUnitRunner.class)
public class KafkaPollerTest {

    @Mock
    private ExceptionEventSender eventSender;
    
    @Mock
    private KafkaConsumer<String, String> consumer;

    @Mock
    private MessageHandler<Person> handler;

    @Test
    public void testHandleError() throws Exception {
        Person person = new Person();
        person.name = "Chris";
        ConsumerRecord<String, String> record = createRecordFor(person);
        when(consumer.poll(Mockito.any()))
            .thenReturn(records(Arrays.asList(record)))
            .thenThrow(new KafkaException("Expected"))
            .thenThrow(new WakeupException());
        doThrow(new RuntimeException("Expected")).when(handler).handle(Mockito.any(MessageInfo.class), Mockito.any(Person.class));
        List<HandlerAdapter<?>> adapters = Collections.singletonList(HandlerAdapter.create(Person.class, handler));
        KafkaPoller poller = new KafkaPoller(consumer, eventSender, adapters);
        poller.errorSleepMs = 100;
        // Should see "Error consuming message" in the log
        verify(handler, timeout(1000)).handle(Mockito.any(MessageInfo.class), Mockito.any(Person.class));
        verify(eventSender, timeout(1000)).send(Mockito.any(KafkaException.class));
        verify(consumer, timeout(1000)).close();
        poller.close();
    }

    private ConsumerRecord<String, String> createRecordFor(Person person) throws JsonProcessingException {
        Headers headers = new RecordHeaders(Collections.singleton(header(KafkaMessageInfo.KEY_MESSAGE_TYPE, Person.class.getSimpleName())));
        ObjectMapper mapper = new ObjectMapper();
        String value = mapper.writerFor(Person.class).writeValueAsString(person);
        return new ConsumerRecord<String, String>(
                "topic", 1, 0l, 0l, TimestampType.CREATE_TIME, 0l, 0, 0, "", value, headers);
    }

    private RecordHeader header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }
    
    private ConsumerRecords<String, String> records(List<ConsumerRecord<String, String>> records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> rm = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            rm.put(new TopicPartition(record.topic(), record.partition()), Arrays.asList(record));
        }
        return new ConsumerRecords<>(rm);
    }
    
}
