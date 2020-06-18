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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.kafka.util.KafkaRule;
import org.apache.sling.distribution.journal.messages.ClearCommand;
import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.SubscriberConfig;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class MessagingTest {

    private String topicName;
    private Semaphore sem = new Semaphore(0);
    private volatile MessageInfo lastInfo;
    
    @ClassRule
    public static KafkaRule kafka = new KafkaRule();
    private MessagingProvider provider;
    private HandlerAdapter<DiscoveryMessage> handler;
    
    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        topicName = "MessagingTest" + UUID.randomUUID().toString();
        this.provider = kafka.getProvider();
        this.handler = HandlerAdapter.create(DiscoveryMessage.class, this::handle);
    }
    
    @Test
    public void testSendReceive() throws Exception {
        try (Closeable poller = provider.createPoller(topicName, Reset.earliest, provider.assignTo(0), handler)) {
            MessageSender<DiscoveryMessage> messageSender = provider.createSender(topicName);
        
            messageSender.send(createMessage());
            assertReceived("Consumer started from earliest .. should see our message");
            messageSender.send(createMessage());
            assertReceived("Should also consume a second message");
        }
    }
    
    @Test
    public void testNoHandler() throws Exception {
        try (Closeable poller = provider.createPoller(topicName, Reset.earliest, handler)) {
            MessageSender<ClearCommand> messageSender = provider.createSender(topicName);
            ClearCommand msg = ClearCommand.builder()
                .subSlingId("subslingid")
                .subAgentName("agentname")
                .build();
            messageSender.send(msg);
            assertNotReceived("Should not be received as we have no handler");
        }
    }
    
    @Test
    public void testAssign() throws Exception {
        DiscoveryMessage msg = createMessage();
        MessageSender<DiscoveryMessage> messageSender = provider.createSender(topicName);
        messageSender.send(msg);
        
        try (Closeable poller = provider.createPoller(topicName, Reset.earliest, handler)) {
            assertReceived("Starting from earliest .. should see our message");
        }
        long offset = lastInfo.getOffset();
        
        String assign = provider.assignTo(offset);
        try (Closeable poller = provider.createPoller(topicName, Reset.latest, assign, handler)) {
            assertReceived("Starting from old offset .. should see our message");
            assertThat(lastInfo.getOffset(), equalTo(offset));
        }
        
        String invalid = provider.assignTo(32532523453l);
        try (Closeable poller1 = provider.createPoller(topicName, Reset.latest, invalid, handler)) {
            assertNotReceived("Should not see message as we fall back to latest");
        }
        
        try (Closeable poller2 = provider.createPoller(topicName, Reset.earliest, invalid, handler)) {
            assertReceived("Should see message as we fall back to earliest");
        }
    }

    private DiscoveryMessage createMessage() {
        return DiscoveryMessage.builder()
                .subAgentName("sub1agent")
                .subSlingId("subsling")
                .subscriberConfiguration(SubscriberConfig
                        .builder()
                        .editable(false)
                        .maxRetries(-1)
                        .build())
                .build();
    }

    private void assertReceived(String message) throws InterruptedException {
        assertTrue(message, sem.tryAcquire(30, TimeUnit.SECONDS));
    }
    
    private void assertNotReceived(String message) throws InterruptedException {
        assertFalse(message, sem.tryAcquire(2, TimeUnit.SECONDS));
    }

    private void handle(MessageInfo info, DiscoveryMessage message) {
        this.lastInfo = info;
        this.sem.release();
    }
    
}
