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
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageInfoTest {
    private static final int PARTITION = 0;
    private static final long OFFSET = 1l;
    private static final long TIMESTAMP = 2l;
    private static final String TOPIC = "topic";
    
    @Mock
    private ConsumerRecord<String, String> record;

    @Test
    public void test() {
        when(record.offset()).thenReturn(OFFSET);
        when(record.partition()).thenReturn(PARTITION);
        when(record.timestamp()).thenReturn(TIMESTAMP);
        when(record.topic()).thenReturn(TOPIC);

        KafkaMessageInfo info = new KafkaMessageInfo(record);
        
        assertThat(info.getOffset(), equalTo(OFFSET));
        assertThat(info.getPartition(), equalTo(PARTITION));
        assertThat(info.getTopic(), equalTo(TOPIC));
        assertThat(info.getCreateTime(), equalTo(TIMESTAMP));
        assertThat(info.toString(), equalTo("Topic: topic, Partition: 0, Offset: 1, CreateTime: 2"));
    }
}
