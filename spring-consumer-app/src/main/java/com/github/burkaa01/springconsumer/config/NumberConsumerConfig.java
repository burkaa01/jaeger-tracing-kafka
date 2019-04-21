package com.github.burkaa01.springconsumer.config;

import io.opentracing.contrib.kafka.TracingConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Collections;
import java.util.Map;

import static org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.MANUAL;

@Configuration
public class NumberConsumerConfig {

    private final KafkaProperties kafkaProperties;

    public NumberConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Integer> numberContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Integer> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.getContainerProperties().setAckMode(MANUAL);
        factory.setConsumerFactory(numberConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<?, ?> kafkaConsumerFactory() {
        Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
        return new DefaultKafkaConsumerFactory(properties);
    }

    @Bean
    public ConsumerFactory<String, Integer> numberConsumerFactory() {
        Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(TracingConsumerInterceptor.class));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(properties);
    }
}
