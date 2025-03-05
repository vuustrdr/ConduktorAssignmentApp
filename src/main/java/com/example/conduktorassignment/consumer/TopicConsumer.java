package com.example.conduktorassignment.consumer;

import com.example.conduktorassignment.dto.Person;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
public class TopicConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicConsumer.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "assignment-consumer";
    private static final int PARTITION_COUNT = 3;

    private final KafkaConsumer<String, String> consumer;


    public TopicConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        this.consumer = new KafkaConsumer<>(props);
    }

    // For test
    public TopicConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public List<Person> consume(String topicName, Integer offset, Integer count) {

        LOGGER.info("Creating 3 partitions for Topic: {}", topicName);

        List<TopicPartition> partitions = new ArrayList<>();

        for (int i = 0; i < PARTITION_COUNT; i++) {
            partitions.add(new TopicPartition(topicName, i));
        }

        LOGGER.info("Assigning partitions to consumer and seeking to Offset {}", offset);

        consumer.assign(partitions);
        for (TopicPartition topicPartition : partitions) {
            consumer.seek(topicPartition, offset);
        }

        LOGGER.info("Polling Topic: {}", topicName);

        ConsumerRecords<String, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));

        List<Person> results = new ArrayList<>();

        // Limit for count requested
        Integer i = count;
        for (ConsumerRecord<String, String> record : records) {

            if (i == 0) break;

            try {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(record.value());
                results.add(new Person(jsonNode.get("_id").asText(),
                        jsonNode.get("name").asText(),
                        jsonNode.get("dob").asText()));
            } catch (Exception e) {
                LOGGER.error("Failed to map record: {}", record.value(), e);
            }

            i--;
        }

        LOGGER.info("Consumed records from Topic {} and returned {} values", topicName, results.size());

        return results;
    }


}
