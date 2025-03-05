package com.example.conduktorassignment.controller;


import com.example.conduktorassignment.consumer.TopicConsumer;
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
    public ResponseEntity<String> getRecordsFromTopic(@PathVariable String topicName, @RequestParam(required = false, defaultValue = "10") Integer count) {

        int defaultOffset = 10;

        LOGGER.info("Received request to get {} record from Topic: {} from Offset: {}", count, topicName, defaultOffset);

        List<String> results = topicConsumer.consume(topicName, defaultOffset, count);

        if (results.isEmpty()) {
            LOGGER.info("No records found from Topic: {} from Offset: {}", topicName, defaultOffset);
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return ResponseEntity.status(HttpStatus.OK).body(prettify(results));
    }

    @GetMapping("/{topicName}/{offset}")
    public ResponseEntity<String> getRecordsFromTopicWithOffset(@PathVariable String topicName, @PathVariable Integer offset, @RequestParam(required = false, defaultValue = "10") Integer count) {
        LOGGER.info("Received request to get {} records from Topic: {} from Offset: {}", count, topicName, offset);

        List<String> results = topicConsumer.consume(topicName, offset, count);

        if (results.isEmpty()) {
            LOGGER.info("No records found from Topic: {} from Offset: {}", topicName, offset);
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return ResponseEntity.status(HttpStatus.OK).body(prettify(results));
    }

    protected String prettify(List<String> results) {
        ObjectMapper mapper = new ObjectMapper();
        List<JsonNode> formattedJsonList = results.stream()
                .map(json -> {
                    try {
                        return mapper.readTree(json);
                    } catch (Exception e) {
                        throw new RuntimeException("Invalid JSON format", e);
                    }
                })
                .collect(Collectors.toList());

        String prettyJson;
        try {
            prettyJson = mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(formattedJsonList);
        } catch (Exception e) {
            throw new RuntimeException("Invalid JSON format", e);
        }


        return prettyJson;

    }

}
