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

import static org.apache.sling.distribution.journal.HandlerAdapter.create;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.messages.Messages.DiscoveryMessage;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessagePollerTest {
    @Mock
    ExceptionEventSender eventSender;
    
    @Mock
    KafkaConsumer<String, byte[]> consumer;

    private Semaphore sem = new Semaphore(0);

    @Test
    public void testNoHeader() throws IOException, InterruptedException {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<String, byte[]>("topic", 1, 0l, 0l, TimestampType.CREATE_TIME, 0, 0, 0, "key", null);
        when(consumer.poll(Mockito.any()))
            .thenReturn(records(Collections.singletonList(record)))
            .thenReturn(records(Collections.emptyList()));
        // Should display java.lang.IllegalArgumentException in log
        try (KafkaMessagePoller poller = new KafkaMessagePoller(consumer, eventSender, create(DiscoveryMessage.class, this::handle))) {
            Assert.assertThat(sem.tryAcquire(100, TimeUnit.MILLISECONDS), equalTo(false));
        }
    }

    private void handle(MessageInfo info, DiscoveryMessage message) {
        sem.release();
    }

    private ConsumerRecords<String, byte[]> records(List<ConsumerRecord<String, byte[]>> records) {
        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> rm = new HashMap<>();
        for (ConsumerRecord<String, byte[]> record : records) {
            rm.put(new TopicPartition(record.topic(), record.partition()), Arrays.asList(record));
        }
        return new ConsumerRecords<>(rm);
    }
}
