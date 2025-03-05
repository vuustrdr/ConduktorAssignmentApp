package com.example.conduktorassignment.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TopicConsumerTest {

    private static final String TOPIC_NAME = "test-topic";

    private TopicConsumer topicConsumer;

    @Mock
    private KafkaConsumer<String, String> consumer;


    @BeforeEach
    void setUp() {
        topicConsumer = new TopicConsumer(consumer);
    }

    @Test
    void testConsume_ReturnsCorrectMessages() {
        int offset = 0;
        int count = 5;

        List<TopicPartition> partitions = Arrays.asList(
                new TopicPartition(TOPIC_NAME, 0),
                new TopicPartition(TOPIC_NAME, 1),
                new TopicPartition(TOPIC_NAME, 2)
        );

        List<ConsumerRecord<String, String>> fakeRecords = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            fakeRecords.add(new ConsumerRecord<>(TOPIC_NAME, i % 3, offset + i, "key" + i, "message" + i));
        }

        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(Map.of(
                partitions.get(0), fakeRecords.subList(0, 2),
                partitions.get(1), fakeRecords.subList(2, 4),
                partitions.get(2), fakeRecords.subList(4, 5)
        ));

        when(consumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        List<String> results = topicConsumer.consume(TOPIC_NAME, offset, count);

        verify(consumer, times(1)).poll(any(Duration.class));
        assertEquals(count, results.size());
    }

}
