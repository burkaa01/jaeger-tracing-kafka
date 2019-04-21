package com.github.burkaa01.stream.divider;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

@Configuration
public class DividerStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(DividerStream.class);
    private static final String TRACING_COMPONENT_NAME = "java-kafka";
    private static final String TRACING_SERVICE_NAME = "kafka";
    private static final String TRACING_NUMBER_VALUE = "number.value";

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
                /* start jaeger trace */
                .transform(tracingTransformerSupplier())
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

    private TransformerSupplier<String, String, KeyValue<String, ValueWithHeaders>> tracingTransformerSupplier() {
        return new TransformerSupplier<String, String, KeyValue<String, ValueWithHeaders>>() {
            public Transformer<String, String, KeyValue<String, ValueWithHeaders>> get() {
                return new Transformer<String, String, KeyValue<String, ValueWithHeaders>>() {

                    private ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public KeyValue<String, ValueWithHeaders> transform(String key, String value) {
                        if (value == null || value.isEmpty()) {
                            return KeyValue.pair(key, null);
                        }

                        injectFirstKafkaConsumerSpan(value, context);

                        return KeyValue.pair(key, new ValueWithHeaders(value, context.headers()));
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
    }

    /**
     * {@link io.opentracing.contrib.kafka.TracingKafkaConsumer} only builds and injects a span when there is a parent
     * context. This is the first consumer and the beginning of our trace (i.e. there is no parent) so we are building
     * and injecting the first span manually. For more context, see the method "buildAndFinishChildSpan" in
     * {@link io.opentracing.contrib.kafka.TracingKafkaUtils}.
     */
    private void injectFirstKafkaConsumerSpan(String number, ProcessorContext context) {
        Tracer tracer = GlobalTracer.get();
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan("receive")
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

        Span span = spanBuilder.start();
        Tags.COMPONENT.set(span, TRACING_COMPONENT_NAME);
        Tags.PEER_SERVICE.set(span, TRACING_SERVICE_NAME);
        span.setTag("partition", context.partition());
        span.setTag("topic", context.topic());
        span.setTag("offset", context.offset());
        /* add the number (aka the first consumer record value) as a span tag */
        span.setTag(TRACING_NUMBER_VALUE, number);
        span.finish();

        TextMap headersMapInjectAdapter = new TextMap() {
            @Override
            public Iterator<Map.Entry<String, String>> iterator() {
                throw new UnsupportedOperationException("iterator should never be used with Tracer.inject()");
            }

            @Override
            public void put(String key, String value) {
                context.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
            }
        };
        tracer.inject(span.context(), Format.Builtin.TEXT_MAP, headersMapInjectAdapter);
    }
}
