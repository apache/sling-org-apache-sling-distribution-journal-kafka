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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.osgi.util.converter.Converters.standardConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.Reset;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaClientProviderTest {
    private static final String TOPIC = "topic";
    
    @Spy
    private KafkaClientProvider provider;
    
    @Mock
    private KafkaConsumer<String, Object> consumer;
    
    @Before
    public void before() {
        doReturn(consumer).when(provider).createConsumer(Mockito.any(), Mockito.any());
        KafkaEndpoint config = createConfig();
        provider.activate(config);
    }

    @Test
    public void testAssertTopicWhenDoesNotExist() throws Exception {
        when(consumer.listTopics()).thenReturn(Collections.emptyMap());
        try {
            provider.assertTopic(TOPIC);
            Assert.fail();
        } catch (MessagingException e) {
            assertThat(e.getMessage(), equalTo("Topic topic does not exist"));
        }
    }

    @Test
    public void testAssertTopicWhenError() throws Exception {
        when(consumer.listTopics()).thenThrow(new RuntimeException("Expected"));
        try {
            provider.assertTopic(TOPIC);
            Assert.fail();
        } catch (MessagingException e) {
            assertThat(e.getMessage(), equalTo("Unable to load topic stats for topic"));
        }
    }
    
    @Test
    public void testAssertTopic() throws Exception {
        Map<String, List<PartitionInfo>> topics = new HashMap<>();
        topics.put(TOPIC, Collections.emptyList());
        when(consumer.listTopics()).thenReturn(topics);
        provider.assertTopic(TOPIC);
    }
    
    @Test
    public void testRetrieveEarliestOffset() throws Exception {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TOPIC, 0), 1l);
        when(consumer.beginningOffsets(Mockito.any())).thenReturn(offsets);
        long offset = provider.retrieveOffset(TOPIC, Reset.earliest);
        assertThat(offset, equalTo(1l));
    }
    
    @Test
    public void testRetrieveLatestOffset() throws Exception {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TOPIC, 0), 1l);
        when(consumer.endOffsets(Mockito.any())).thenReturn(offsets);
        long offset = provider.retrieveOffset(TOPIC, Reset.latest);
        assertThat(offset, equalTo(1l));
    }
    
    @Test
    public void testAssignTo() throws Exception {
        String assign = provider.assignTo(1l);
        assertThat(assign, equalTo("0:1"));
    }

    private KafkaEndpoint createConfig() {
        Map<String, String> props = new HashMap<>();
        KafkaEndpoint config = standardConverter().convert(props).to(KafkaEndpoint.class);
        return config;
    }
}
