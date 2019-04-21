package com.github.burkaa01.springconsumer.config;

import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.contrib.spring.web.client.HttpHeadersCarrier;
import io.opentracing.contrib.spring.web.client.RestTemplateSpanDecorator;
import io.opentracing.contrib.spring.web.client.TracingRestTemplateInterceptor;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

/**
 * A version of OpenTracing's Spring RestTemplate integration {@link TracingRestTemplateInterceptor} to be used when
 * a request is a child of another span in the trace. In our case, the request in question is a child of a span from
 * our Kafka consumer.
 */
public class TracingChildRestTemplateInterceptor implements ClientHttpRequestInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TracingChildRestTemplateInterceptor.class);
    private static final RestTemplateSpanDecorator REST_SPAN_DECORATOR = new RestTemplateSpanDecorator.StandardTags();

    private Tracer tracer;
    private Headers headers;

    public TracingChildRestTemplateInterceptor(Headers headers) {
        this.tracer = GlobalTracer.get();
        this.headers = headers;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest httpRequest, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        ClientHttpResponse httpResponse;

        SpanContext parentContext = null;
        if (headers != null && headers.toArray().length > 0) {
            parentContext = TracingKafkaUtils.extractSpanContext(headers, tracer);
        }

        try (Scope scope = tracer.buildSpan(httpRequest.getMethod().toString())
                .asChildOf(parentContext)
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                .startActive(true)) {

            tracer.inject(scope.span().context(), Format.Builtin.HTTP_HEADERS,
                    new HttpHeadersCarrier(httpRequest.getHeaders()));

            try {
                REST_SPAN_DECORATOR.onRequest(httpRequest, scope.span());
            } catch (RuntimeException exception) {
                LOGGER.error("Exception decorating span", exception);
            }

            try {
                httpResponse = execution.execute(httpRequest, body);
            } catch (Exception exception) {
                try {
                    REST_SPAN_DECORATOR.onError(httpRequest, exception, scope.span());
                } catch (RuntimeException ex) {
                    LOGGER.error("Exception decorating span", ex);
                }
                throw exception;
            }

            try {
                REST_SPAN_DECORATOR.onResponse(httpRequest, httpResponse, scope.span());
            } catch (RuntimeException exception) {
                LOGGER.error("Exception decorating span", exception);
            }
        }
        return httpResponse;
    }
}
