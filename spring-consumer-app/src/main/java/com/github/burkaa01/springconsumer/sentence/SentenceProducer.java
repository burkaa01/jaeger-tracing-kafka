package com.github.burkaa01.springconsumer.sentence;

import com.github.burkaa01.springconsumer.config.TracingChildRestTemplateInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

@Component
public class SentenceProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SentenceProducer.class);
    private static final String TRACING_SPAN_CONTEXT_KEY = "uber-trace-id";
    private static final String TRACING_SECOND_SPAN_CONTEXT_KEY = "second_span_uber-trace-id";

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
        HttpEntity<String> response = sendToRestApp(url, record);
        String restAppResponse = response.getBody();
        LOGGER.info("{}: value={}", url, restAppResponse);

        updateTracingHeaders(record, response.getHeaders());

        int secondNumber;
        try {
            secondNumber = Integer.parseInt(restAppResponse);
        } catch (NumberFormatException e) {
            return;
        }

        /* produce resulting sentence and send to sentence topic */
        produceSentence(number, secondNumber, record.headers());

        /* acknowledge message */
        acks.acknowledge();
    }

    private HttpEntity<String> sendToRestApp(String url, ConsumerRecord<String, Integer> record) {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setInterceptors(Collections.singletonList(new TracingChildRestTemplateInterceptor(record.headers())));
        return restTemplate.exchange(url, HttpMethod.POST, null, String.class);
    }

    private void produceSentence(int firstNumber, int secondNumber, Iterable<Header> headers) {
        String sentence = firstNumber + " + " + secondNumber;
        LOGGER.info("{}: value={}", sentenceTopic, sentence);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                sentenceTopic,
                null,
                null,
                sentence,
                headers);
        sentenceTemplate.send(producerRecord);
    }

    private void updateTracingHeaders(ConsumerRecord<String, Integer> record, HttpHeaders httpHeaders) {
        String spanContext = spanContextFromHttpHeaders(httpHeaders);
        if (spanContext != null) {
            record.headers().remove(TRACING_SPAN_CONTEXT_KEY);
            record.headers().remove(TRACING_SECOND_SPAN_CONTEXT_KEY);
            record.headers().add(TRACING_SPAN_CONTEXT_KEY, spanContext.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String spanContextFromHttpHeaders(HttpHeaders headers) {
        List<String> spanContextHeader = headers.get(TRACING_SPAN_CONTEXT_KEY);
        if (spanContextHeader == null || spanContextHeader.isEmpty()) {
            return null;
        }
        try {
            return URLDecoder.decode(String.join("", spanContextHeader), StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException exception) {
            LOGGER.error("Exception reading span context from HttpHeader", exception);
            return null;
        }
    }
}
