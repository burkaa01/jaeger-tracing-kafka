package com.github.burkaa01.rest.controller;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class RandomRestController {

    private static final Logger LOGGER = LoggerFactory.getLogger(RandomRestController.class);
    private static final String RANDOM_NUMBER_TAG = "random.number";

    @PostMapping(path = "/random", produces = {"application/json"})
    public ResponseEntity<String> example() {

        int randomNumber = new Random().nextInt(10);
        LOGGER.info("{} was {}", RANDOM_NUMBER_TAG, randomNumber);

        Tracer tracer = GlobalTracer.get();
        Span span = tracer.activeSpan();
        span.setTag(RANDOM_NUMBER_TAG, randomNumber);

        return new ResponseEntity<>(Integer.toString(randomNumber), HttpStatus.OK);
    }
}
