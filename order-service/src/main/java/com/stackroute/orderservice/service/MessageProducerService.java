package com.stackroute.orderservice.service;

import com.stackroute.orderservice.dto.OrderDto;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * MessageProducerService class should be used to send messages to Kafka Topic
 * **TODO**
 * This class should use KafkaTemplate to send OrderDto message
 */

@Slf4j
public class MessageProducerService {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducerService.class);

    @Value("${kafka.topic-name}")
    private String topic;

    /**
     * **TODO**
     * Inject a bean of KafkaTemplate created in KafkaConfig class
     */
    @Autowired
    private KafkaTemplate<String, OrderDto> kafkaTemplate;

    /**
     * **TODO**
     * Create a method sendOrderMessage(OrderDto message)
     * to send order details message to Kafka topic
     */
    public void sendOrderMessage(OrderDto message){
        ListenableFuture<SendResult<String, OrderDto>> future = kafkaTemplate.send(topic, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, OrderDto>>() {
            @Override
            public void onFailure(Throwable exception) {
                log.error(exception.getLocalizedMessage());
            }

            @Override
            public void onSuccess(SendResult<String, OrderDto> result) {
                log.info("Successfully posted on Kafka topic!!");
                log.info("Offset : "+result.getRecordMetadata().offset());


            }
        });
    }
}