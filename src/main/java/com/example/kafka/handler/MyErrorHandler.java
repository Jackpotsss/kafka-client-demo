package com.example.kafka.handler;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service("myErrorHandler")
public class MyErrorHandler  implements KafkaListenerErrorHandler {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
        logger.info(message.getPayload().toString());
        return null;
    }

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        logger.info(message.getPayload().toString());
        return KafkaListenerErrorHandler.super.handleError(message, exception, consumer);
    }
}
