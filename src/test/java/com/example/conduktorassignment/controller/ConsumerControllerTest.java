package com.example.conduktorassignment.controller;

import com.example.conduktorassignment.consumer.TopicConsumer;
import com.example.conduktorassignment.dto.Person;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ConsumerController.class)
public class ConsumerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Mock
    private KafkaConsumer<String, String> consumer;

    @MockitoBean
    private TopicConsumer topicConsumer;

    private static final String TOPIC_NAME = "test-topic";

    @BeforeEach
    void setUp() {
        Mockito.reset(topicConsumer);
    }

    @Test
    void testGetRecordsFromTopic_Success() throws Exception {

        List<Person> mockMessages = List.of(new Person("123", "a", "1010"));
        when(topicConsumer.consume(TOPIC_NAME, 10, 10)).thenReturn(mockMessages);

        mockMvc.perform(get("/topic/" + TOPIC_NAME))
                .andExpect(status().isOk())
                .andExpect(content().json("[{\"_id\":\"123\",\"name\":\"a\",\"dob\":\"1010\"}]"));

        verify(topicConsumer, times(1)).consume(TOPIC_NAME, 10, 10);

    }


}
