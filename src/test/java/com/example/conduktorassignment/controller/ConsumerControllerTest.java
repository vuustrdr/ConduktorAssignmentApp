package com.example.conduktorassignment.controller;

import com.example.conduktorassignment.consumer.TopicConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ConsumerController.class)
class ConsumerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @InjectMocks
    private TopicConsumer topicConsumer;

    private static final String TOPIC_NAME = "test-topic";

    @BeforeEach
    void setUp() {
        Mockito.reset(topicConsumer);
    }

    @Test
    void testGetRecordsFromTopic_Success() throws Exception {

        List<String> mockMessages = List.of("message1", "message2", "message3");
        when(topicConsumer.consume(TOPIC_NAME, 10, 10)).thenReturn(mockMessages);

        mockMvc.perform(get("/topic/" + TOPIC_NAME))
                .andExpect(status().isOk())
                .andExpect(content().json("[\"message1\",\"message2\",\"message3\"]"));

        verify(topicConsumer, times(1)).consume(TOPIC_NAME, 10, 10);

    }


}
