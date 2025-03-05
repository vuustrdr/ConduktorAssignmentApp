package com.example.conduktorassignment.controller;


import com.example.conduktorassignment.consumer.TopicConsumer;
import com.example.conduktorassignment.dto.Person;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/topic")
public class ConsumerController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerController.class);

    private final TopicConsumer topicConsumer;

    @Autowired
    public ConsumerController(TopicConsumer topicConsumer) {
        this.topicConsumer = topicConsumer;
    }

    // Repeated code here just for the sake of clarity
    @GetMapping("/{topicName}")
    public ResponseEntity<List<Person>> getRecordsFromTopic(@PathVariable String topicName, @RequestParam(required = false, defaultValue = "10") Integer count) {

        int defaultOffset = 10;

        LOGGER.info("Received request to get {} record from Topic: {} from Offset: {}", count, topicName, defaultOffset);

        List<Person> results = topicConsumer.consume(topicName, defaultOffset, count);

        if (results.isEmpty()) {
            LOGGER.info("No records found from Topic: {} from Offset: {}", topicName, defaultOffset);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ArrayList<>());
        }

        return ResponseEntity.status(HttpStatus.OK).body(results);
    }

    @GetMapping("/{topicName}/{offset}")
    public ResponseEntity<List<Person>> getRecordsFromTopicWithOffset(@PathVariable String topicName, @PathVariable Integer offset, @RequestParam(required = false, defaultValue = "10") Integer count) {
        LOGGER.info("Received request to get {} records from Topic: {} from Offset: {}", count, topicName, offset);

        List<Person> results = topicConsumer.consume(topicName, offset, count);

        if (results.isEmpty()) {
            LOGGER.info("No records found from Topic: {} to Offset: {}", topicName, offset);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ArrayList<>());
        }

        return ResponseEntity.status(HttpStatus.OK).body(results);
    }

}
