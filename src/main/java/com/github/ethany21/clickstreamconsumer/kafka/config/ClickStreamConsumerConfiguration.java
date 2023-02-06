package com.github.ethany21.clickstreamconsumer.kafka.config;

import clickstream.events;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;


@Configuration
public class ClickStreamConsumerConfiguration {

    @Value(value = "${spring.kafka.properties.bootstrap.servers}")
    private String bootstrapAddress;
    @Value(value = "${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;
    @Value(value = "${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;
    @Value(value = "${spring.kafka.properties.security.protocol}")
    private String securityProtocol;
    @Value(value = "${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;
    @Value(value = "${spring.kafka.properties.basic.auth.credentials.source}")
    private String basicAuthCredentialsSource;
    @Value(value = "${spring.kafka.properties.basic.auth.user.info}")
    private String basicAuthUserInfo;


    @Bean
    public Map<String, Object> clickStreamConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "clickStream_consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("basic.auth.user.info", basicAuthUserInfo);
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.jaas.config", saslJaasConfig);
        props.put("security.protocol", securityProtocol);
        props.put("basic.auth.credentials.source", basicAuthCredentialsSource);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return props;
    }

    @Bean
    public ConsumerFactory<String, events> clickStreamConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(clickStreamConsumerConfig());
    }

    @Bean("clickStreamKafkaListenerContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, events>> clickStreamKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, events> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(clickStreamConsumerFactory());
        factory.setBatchListener(true);
        factory.setConcurrency(6);
        return factory;

    }

}
