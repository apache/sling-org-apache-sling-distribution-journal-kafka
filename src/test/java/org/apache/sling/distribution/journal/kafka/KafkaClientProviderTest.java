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

import static java.util.Collections.emptyMap;
import static org.apache.sling.distribution.journal.kafka.util.KafkaEndpointBuilder.buildKafkaEndpoint;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.Reset;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.service.event.EventAdmin;

@RunWith(MockitoJUnitRunner.class)
public class KafkaClientProviderTest {
    private static final String TOPIC = "topic";
    
    @Mock
    EventAdmin eventAdmin;
    
    private KafkaClientProvider provider;
    
    @Mock
    private KafkaConsumer<String, Object> consumer;
    
    @Before
    public void before() {
        KafkaEndpoint config = buildKafkaEndpoint(emptyMap());
        provider = Mockito.spy(new KafkaClientProvider(eventAdmin, config));
        doReturn(consumer).when(provider).createConsumer(Mockito.any(), Mockito.any());
    }

    @Test
    public void testAssertTopicWhenDoesNotExist() throws Exception {
        when(consumer.listTopics()).thenReturn(emptyMap());
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
    
    @Test
    public void testAssignToRelative() throws Exception {
        String assign = provider.assignTo(Reset.latest, -1l);
        assertThat(assign, equalTo("0:latest:-1"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAssign() throws Exception {
        HandlerAdapter<?> handler = Mockito.mock(HandlerAdapter.class);
        provider.createPoller(TOPIC, Reset.latest, "", handler);
    }
    
    @Test
    public void testGeServerURI() throws Exception {
        URI serverUri = provider.getServerUri();
        assertThat(serverUri.toString(), equalTo("localhost:9092"));
    }
}
