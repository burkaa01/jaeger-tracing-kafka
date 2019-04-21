package com.github.burkaa01.stream.divider;

import org.apache.kafka.common.header.Headers;

public class ValueWithHeaders {

    private final String value;
    private final Headers headers;

    public ValueWithHeaders(String value, Headers headers) {
        this.value = value;
        this.headers = headers;
    }

    public String getValue() {
        return value;
    }

    public Headers getHeaders() {
        return headers;
    }
}
