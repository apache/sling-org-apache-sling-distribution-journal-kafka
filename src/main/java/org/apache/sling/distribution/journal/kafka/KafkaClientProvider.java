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
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.sling.distribution.journal.ExceptionEventSender;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = MessagingProvider.class, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = KafkaEndpoint.class)
public class KafkaClientProvider implements MessagingProvider, Closeable {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public static final int PARTITION = 0;
    
    private final ExceptionEventSender eventSender;

    private final String kafkaBootstrapServers;

    private final int requestTimeout;

    private final int defaultApiTimeout;

    private final String securityProtocol;

    private final String saslMechanism;

    private final String saslJaasConfig;

    private final URI serverUri;

    private transient KafkaProducer<String, String> jsonProducer = null;

    @Activate
    public KafkaClientProvider(
            @Reference EventAdmin eventAdmin, 
            KafkaEndpoint kafkaEndpoint
            ) {
        eventSender = new ExceptionEventSender(eventAdmin);
        kafkaBootstrapServers = requireNonNull(kafkaEndpoint.kafkaBootstrapServers());
        String[] servers = kafkaBootstrapServers.split(",");
        try {
            serverUri = new URI(servers[0]);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        requestTimeout = kafkaEndpoint.kafkaRequestTimeout();
        defaultApiTimeout = kafkaEndpoint.kafkaDefaultApiTimeout();
        securityProtocol = kafkaEndpoint.securityProtocol();
        saslMechanism = kafkaEndpoint.saslMechanism();
        saslJaasConfig = kafkaEndpoint.saslJaasConfig();
    }
    
    @Deactivate
    public synchronized void close() {
        closeQuietly(jsonProducer);
    }

    @Override
    public <T> MessageSender<T> createSender(String topic) {
        return new KafkaJsonMessageSender<>(buildJsonKafkaProducer(), topic, eventSender);
    }

    @Override
    public Closeable createPoller(String topicName, Reset reset, @Nullable String assign, HandlerAdapter<?>... adapters) {
        log.info("Creating poller for topic={}, reset={}, assing={} with adapters {}.", topicName, reset, assign, adapters);
        KafkaConsumer<String, String> consumer = createConsumer(StringDeserializer.class, reset);
        TopicPartition topicPartition = new TopicPartition(topicName, PARTITION);
        Collection<TopicPartition> topicPartitions = singleton(topicPartition);
        consumer.assign(topicPartitions);
        if (assign != null) {
            AssignDetails assignDetails = new AssignDetails(assign);
            long offset = assignDetails.getOffset(consumer, topicPartition);
            consumer.seek(topicPartition, offset);
        } else if (reset == Reset.earliest) {
            consumer.seekToBeginning(topicPartitions);
        } else {
            consumer.seekToEnd(topicPartitions);
        }
        return new KafkaPoller(consumer, eventSender, Arrays.asList(adapters));
    }

    @Override
    public void assertTopic(String topic) throws MessagingException {
        Map<String, List<PartitionInfo>> topics;
        try (KafkaConsumer<String, String> consumer = createConsumer(StringDeserializer.class, Reset.latest)) {
            topics = consumer.listTopics();
        } catch (Exception e) {
            throw new MessagingException(format("Unable to load topic stats for %s", topic), e);
        }
        if (! topics.containsKey(topic)) {
            throw new MessagingException(format("Topic %s does not exist", topic));
        }
    }

    @Override
    public long retrieveOffset(String topicName, Reset reset) {
        try (KafkaConsumer<String, String> consumer = createConsumer(StringDeserializer.class, reset)) {
            TopicPartition topicPartition = new TopicPartition(topicName, PARTITION);
            Map<TopicPartition, Long> offsets = getOffsets(reset, consumer, topicPartition);
            return offsets.get(topicPartition);
        }
    }

    private Map<TopicPartition, Long> getOffsets(Reset reset, KafkaConsumer<String, String> consumer,
            TopicPartition topicPartition) {
        Collection<TopicPartition> topicPartitions = singleton(topicPartition);
        return (reset == Reset.earliest) //
                ? consumer.beginningOffsets(topicPartitions)
                : consumer.endOffsets(topicPartitions);
    }

    @Override
    public String assignTo(long offset) {
        return format("%s:%s", PARTITION, offset);
    }
    
    @Override
    public String assignTo(Reset reset, long relativeOffset) {
        return format("%s:%s:%d", PARTITION, reset.name(), relativeOffset);
    }

    protected <T> KafkaConsumer<String, T> createConsumer(Class<? extends Deserializer<?>> deserializer, Reset reset) {
        String groupId = UUID.randomUUID().toString();
        try (CLSwitch switcher = new CLSwitch(KafkaConsumer.class.getClassLoader())) {
            return new KafkaConsumer<>(consumerConfig(deserializer, groupId, reset));
        }
    }

    private void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final IOException ioe) {
            // ignore
        }
    }

    @Nonnull
    private synchronized KafkaProducer<String, String> buildJsonKafkaProducer() {
        if (jsonProducer == null) {
            jsonProducer = new KafkaProducer<>(producerConfig(StringSerializer.class));
        }
        return jsonProducer;
    }

    private Map<String, Object> consumerConfig(Object deserializer, String consumerGroupId, Reset reset) {
        Map<String, Object> config = commonConfig();
        config.put(GROUP_ID_CONFIG, consumerGroupId);
        config.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeout);
        config.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        config.put(AUTO_OFFSET_RESET_CONFIG, reset.name());
        return config;
    }

    private Map<String, Object> producerConfig(Object serializer) {
        Map<String, Object> config = commonConfig();
        config.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        config.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        config.put(ACKS_CONFIG, "all");
        return unmodifiableMap(config);
    }

    private Map<String, Object> commonConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        config.put(SASL_MECHANISM, saslMechanism);
        config.put(SECURITY_PROTOCOL_CONFIG, securityProtocol);
        config.put(SASL_JAAS_CONFIG, saslJaasConfig);
        return config;
    }

    @Override
    public URI getServerUri() {
        return serverUri;
    }
    
    static class AssignDetails {
        private final Reset reset;
        private final long offset;

        AssignDetails(String assign) {
            String[] chunks = assign.split(":");
            if (chunks.length == 3) {
                String resetSt = chunks[1];
                this.reset = Reset.valueOf(resetSt);
                offset = Long.parseLong(chunks[2]);
            } else if (chunks.length == 2) {
                reset = null;
                offset = Long.parseLong(chunks[1]);
            } else {
                throw new IllegalArgumentException(format("Illegal assign %s", assign));
            }
        }

        long getOffset(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
            Collection<TopicPartition> partitions = singleton(topicPartition);
            if (reset == Reset.earliest) {
                return consumer.beginningOffsets(partitions).get(topicPartition) + offset;
            } else if (reset == Reset.latest) {
                return consumer.endOffsets(partitions).get(topicPartition) + offset;
            } else {
                return offset;
            }
        }
    }
}
