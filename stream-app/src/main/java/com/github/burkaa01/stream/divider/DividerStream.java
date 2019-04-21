package com.github.burkaa01.stream.divider;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DividerStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(DividerStream.class);

    @Value("${topics.source-topic}")
    private String sourceTopic;
    @Value("${topics.target-topic}")
    private String targetTopic;

    private final DividerMapper dividerMapper;

    public DividerStream(DividerMapper dividerMapper) {
        this.dividerMapper = dividerMapper;
    }

    @Bean
    public KStream<String, Integer> numberStream(StreamsBuilder streamsBuilder) {
        KStream<String, Integer> stream = streamsBuilder
                /* create stream of string numbers */
                .stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()))
                /* log record received */
                .peek((key, value) -> LOGGER.info("{}: value={}", sourceTopic, value))
                /* map to even integers */
                .map(dividerMapper::map)
                /* log record to send */
                .peek(((key, value) -> LOGGER.info("{}: value={}", targetTopic, value)))
                /* filter nulls coming from map */
                .filter(((key, value) -> value != null));

        /* send even integers to target topic */
        stream.to(targetTopic, Produced.with(Serdes.String(), Serdes.Integer()));
        return stream;
    }
}
