package com.github.burkaa01.consumer.config;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Properties;

@Configuration
public class SentenceConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    String bootstrapServers;
    @Value("${kafka.consumer.enable-auto-commit}")
    String enableAutoCommit;
    @Value("${kafka.consumer.auto-offset-reset}")
    String autoOffsetReset;
    @Value("${kafka.consumer.max-poll-records}")
    String maxPollRecords;
    @Value("${kafka.consumer.group-id}")
    String groupId;
    @Value("${kafka.consumer.client-id}")
    String clientId;

    @Value("${topics.source-topic}")
    String sentenceTopic;

    private final Tracer tracer;

    public SentenceConsumerConfig(Tracer tracer) {
        this.tracer = tracer;
    }

    @Bean
    public TracingKafkaConsumer<String, String> sentenceKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(sentenceTopic));
        return new TracingKafkaConsumer<>(consumer, tracer);
    }
}
