package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class ConsumerServiceImpl {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @KafkaListener(id = "webGroup", topicPartitions = {
            @TopicPartition(topic = "topic1", partitions = {"0", "1"}),
            @TopicPartition(topic = "topic2", partitions = "0",
            partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "100"))
    },concurrency = "6",errorHandler = "myErrorHandler")
    public void listen(String input, Acknowledgment ack) {
        logger.info("input value: {}" , input);
        
    }
}
