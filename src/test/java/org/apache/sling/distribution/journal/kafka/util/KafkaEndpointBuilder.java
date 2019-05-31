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
package org.apache.sling.distribution.journal.kafka.util;

import java.util.Map;

import org.apache.sling.distribution.journal.kafka.KafkaEndpoint;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.osgi.util.converter.Converters.standardConverter;

public class KafkaEndpointBuilder {

    public static KafkaEndpoint buildKafkaEndpoint(Map<String, Object> props) {

        /*
         * The standardConverter does not support null default
         * Until FELIX-6137 is fixed, we use this 'creative' way
         * to build KafkaEndpoint.
         */

        KafkaEndpoint proxy = standardConverter().convert(props).to(KafkaEndpoint.class);
        KafkaEndpoint endpoint = mock(KafkaEndpoint.class);
        when(endpoint.saslJaasConfig()).thenReturn(null);
        when(endpoint.securityProtocol()).thenReturn(proxy.securityProtocol());
        when(endpoint.kafkaBootstrapServers()).thenReturn(proxy.kafkaBootstrapServers());
        when(endpoint.kafkaDefaultApiTimeout()).thenReturn(proxy.kafkaDefaultApiTimeout());
        when(endpoint.kafkaRequestTimeout()).thenReturn(proxy.kafkaRequestTimeout());
        when(endpoint.saslMechanism()).thenReturn(proxy.saslMechanism());
        return endpoint;
    }
}
