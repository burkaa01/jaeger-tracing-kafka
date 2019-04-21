package com.github.burkaa01.consumer.answer;

import com.google.gson.JsonObject;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class AnswerProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnswerProducer.class);

    @Value("${topics.source-topic}")
    String sentenceTopic;
    @Value("${topics.target-topic}")
    String answerTopic;

    private final TracingKafkaConsumer<String, String> sentenceKafkaConsumer;
    private final TracingKafkaProducer<String, String> answerKafkaProducer;

    public AnswerProducer(TracingKafkaConsumer<String, String> sentenceKafkaConsumer, TracingKafkaProducer<String, String> answerKafkaProducer) {
        this.sentenceKafkaConsumer = sentenceKafkaConsumer;
        this.answerKafkaProducer = answerKafkaProducer;
    }

    @EventListener(ContextRefreshedEvent.class)
    public void contextRefreshedEvent() {
        listen();
    }

    private void listen() {
        boolean forever = true;
        while (forever) {

            ConsumerRecords<String, String> records = sentenceKafkaConsumer.poll(100L);
            int recordCount = records.count();

            for (ConsumerRecord<String, String> record : records) {
                String sentence = record.value();
                LOGGER.info("{}: value={}", sentenceTopic, sentence);
                produceAnswer(sentence);
            }

            if (recordCount > 0) {
                sentenceKafkaConsumer.commitSync();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException exception) {
                    LOGGER.error("Interrupted exception", exception);
                }
            }
        }
        answerKafkaProducer.close();
    }

    private void produceAnswer(String sentence) {
        int firstNumber;
        int secondNumber;
        String[] numbers = sentence.split(" \\+ ", 2);
        try {
            firstNumber = Integer.parseInt(numbers[0]);
            secondNumber = Integer.parseInt(numbers[1]);
        } catch (NumberFormatException exception) {
            LOGGER.error("Exception parsing sentence", exception);
            return;
        }
        int answer = firstNumber + secondNumber;
        String completeSentence = firstNumber + " + " + secondNumber + " = " + answer;

        JsonObject json = new JsonObject();
        json.addProperty("number", firstNumber);
        json.addProperty("random", secondNumber);
        json.addProperty("sentence", sentence);
        json.addProperty("completeSentence", completeSentence);
        json.addProperty("answer", answer);
        String jsonString = json.toString();

        ProducerRecord<String, String> record = new ProducerRecord<>(answerTopic, null, jsonString);
        LOGGER.info("{}: value={}", answerTopic, jsonString);

        answerKafkaProducer.send(record, (RecordMetadata recordMetadata, Exception exception) -> {
            if (exception != null) {
                LOGGER.error("Exception producing answer", exception);
            }
        });
    }
}
