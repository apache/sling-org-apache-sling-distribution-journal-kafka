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

import static org.apache.sling.distribution.journal.kafka.util.KafkaEndpointBuilder.buildKafkaEndpoint;

import java.util.HashMap;
import java.util.Map;

import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.kafka.KafkaClientProvider;
import org.apache.sling.distribution.journal.kafka.KafkaEndpoint;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class KafkaRule implements TestRule {
    private KafkaClientProvider provider;

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                runWithKafka(base);
            }
        };
    }

    private void runWithKafka(Statement base) throws Throwable {
        try (KafkaLocal kafka = new KafkaLocal()) {
            this.provider = createProvider();
            base.evaluate();
            this.provider.close();
        }
    }

    private KafkaClientProvider createProvider() {
        Map<String, Object> props = new HashMap<>();
        props.put("connectTimeout", "5000");
        props.put("saslJaasConfig", "");
        KafkaEndpoint config = buildKafkaEndpoint(props);
        return new KafkaClientProvider(null, config);
    }
    
    public MessagingProvider getProvider() {
        if (this.provider == null) {
            this.provider = createProvider();
        }
        return provider;
    }
}
