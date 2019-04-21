package com.github.burkaa01.springconsumer.sentence;

import io.opentracing.contrib.spring.web.client.TracingRestTemplateInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

@Component
public class SentenceProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SentenceProducer.class);

    @Value("${topics.source-topic}")
    String numberTopic;
    @Value("${topics.target-topic}")
    String sentenceTopic;
    @Value("${rest-app.url}")
    String restAppUrl;
    @Value("${rest-app.api}")
    String restAppApi;

    private final KafkaTemplate<String, String> sentenceTemplate;

    public SentenceProducer(KafkaTemplate<String, String> sentenceTemplate) {
        this.sentenceTemplate = sentenceTemplate;
    }

    @KafkaListener(topics = "${topics.source-topic}", containerFactory = "numberContainerFactory")
    public void listen(ConsumerRecord<String, Integer> record, Acknowledgment acks) {
        if (record.value() == null) {
            return;
        }
        int number = record.value();
        LOGGER.info("{}: value={}", numberTopic, number);

        /* call rest app */
        final String url = restAppUrl + restAppApi;
        HttpEntity<String> response = sendToRestApp(url);
        String restAppResponse = response.getBody();
        LOGGER.info("{}: value={}", url, restAppResponse);

        int secondNumber;
        try {
            secondNumber = Integer.parseInt(restAppResponse);
        } catch (NumberFormatException e) {
            return;
        }

        /* produce resulting sentence and send to sentence topic */
        produceSentence(number, secondNumber);

        /* acknowledge message */
        acks.acknowledge();
    }

    private HttpEntity<String> sendToRestApp(String url) {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setInterceptors(Collections.singletonList(new TracingRestTemplateInterceptor()));
        return restTemplate.exchange(url, HttpMethod.POST, null, String.class);
    }

    private void produceSentence(int firstNumber, int secondNumber) {
        String sentence = firstNumber + " + " + secondNumber;
        LOGGER.info("{}: value={}", sentenceTopic, sentence);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                sentenceTopic,
                null,
                sentence);
        sentenceTemplate.send(producerRecord);
    }
}
