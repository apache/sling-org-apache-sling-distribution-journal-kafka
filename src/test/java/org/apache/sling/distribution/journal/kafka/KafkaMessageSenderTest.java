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

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.messages.Messages.ClearCommand;
import org.apache.sling.distribution.journal.messages.Messages.PingMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.protobuf.GeneratedMessage;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageSenderTest {

    private static final String TOPIC = "topic";

    @Mock
    private KafkaProducer<String, byte[]> producer;
    
    @InjectMocks
    private KafkaMessageSender<GeneratedMessage> sender;

    @Mock
    private Future<RecordMetadata> record;
    

    @Test(expected = IllegalArgumentException.class)
    public void testNoMapping() throws Exception {
        GeneratedMessage payload = ClearCommand.newBuilder().setOffset(0l).build();
        sender.send(TOPIC, payload);
    }
    
    @Test(expected = MessagingException.class)
    public void testSendError() throws Exception {
        when(producer.send(Mockito.any())).thenReturn(record);
        when(record.get()).thenThrow(new ExecutionException(new IOException("Expected")));
        GeneratedMessage payload = PingMessage.newBuilder().build();
        sender.send(TOPIC, payload);
    }
}
