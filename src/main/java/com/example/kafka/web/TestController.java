package com.example.kafka.web;

import com.example.kafka.service.ProducerServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test/kafka")
public class TestController {

    @Autowired
    private ProducerServiceImpl producerService;

    private Logger logger = LoggerFactory.getLogger(getClass());

    @GetMapping("/send/{input}")
    public void sendFoo(@PathVariable String input) {
        producerService.sendSync("topic_input", input);
    }
    @KafkaListener(id = "webGroup", topics = "topic_input")
    public void listen(String input) {
        logger.info("input value: {}" , input);
    }
}
