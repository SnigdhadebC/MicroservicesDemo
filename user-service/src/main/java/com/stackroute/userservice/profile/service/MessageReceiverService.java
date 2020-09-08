package com.stackroute.userservice.profile.service;

import com.stackroute.userservice.errorhandling.exception.CustomerNotFoundException;
import com.stackroute.userservice.profile.dto.OrderDto;
import com.stackroute.userservice.profile.model.Order;
import org.modelmapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

/**
 * MessageProducerService class should be used to receive messages from Kafka Topic
 * **TODO**
 * Annotate this class to create a bean of type "Service"
 */

@Service
public class MessageReceiverService {
    /**
     * **TODO**
     * Create a slf4j Logger for logging messages to standard output
     */
    private Logger log = LoggerFactory.getLogger(MessageReceiverService.class);

    private final CustomerService customerService;
    private final ModelMapper modelMapper;

    private final CountDownLatch latch = new CountDownLatch(1);

    @Autowired
    public MessageReceiverService(CustomerService customerService, ModelMapper modelMapper) {
        this.customerService = customerService;
        this.modelMapper = modelMapper;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    /**
     * TODO**
     * Create a method receive(OrderDto message)
     * to receive order details message from Kafka topic
     * Use KafkaListener annotation to achieve the same
     * On receipt of the message, count down the latch value(useful for testing receipt of message)
     * Update the order in the customer Document in MongoDb using the customerService
     * Log error messages in case of failure, log informational with message deatils
     * when message is successfully received
     */

    @KafkaListener(topics = "orders",containerFactory = "kafkaListenerContainerFactory")
    public void receive(OrderDto message){
        log.info("Message : "+message);
        try{
            latch.countDown();
            Order order = convertToOrderEntity(message);
            customerService.updateCustomerOrder(order);
        }
        catch (CustomerNotFoundException e) {
            log.error(e.getMessage());
        }
        catch(Throwable exception){
            log.error("receive message from Kafka : "+message);
            log.error(exception.getLocalizedMessage());
        }

    }


    /**
     * TODO**
     * Complete the method to convert OrderDTO in to Order Entity
     */
    Order convertToOrderEntity(OrderDto orderDto) {
        return modelMapper.map(orderDto,Order.class);
    }
}
