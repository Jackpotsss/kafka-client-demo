package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;

@Configuration
public class KafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public KafkaAdmin kafkaAdmin(KafkaProperties properties){
        KafkaAdmin admin = new KafkaAdmin(properties.buildAdminProperties());
        admin.setFatalIfBrokerNotAvailable(true);
        return admin;
    }

    @Bean
    public ProducerFactory producerFactory() {
        DefaultKafkaProducerFactory kafkaProducerFactory = new DefaultKafkaProducerFactory(kafkaProperties.buildProducerProperties());
        kafkaProducerFactory.transactionCapable();
        kafkaProducerFactory.setTransactionIdPrefix("trans-");
        return kafkaProducerFactory;
    }

    @Bean
    public KafkaTransactionManager transactionManager(ProducerFactory producerFactory) {
        KafkaTransactionManager manager = new KafkaTransactionManager(producerFactory);
        return manager;
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, String> repliesContainer(ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {
        ConcurrentMessageListenerContainer<String, String> repliesContainer = containerFactory.createContainer("replies");
        repliesContainer.getContainerProperties().setGroupId("repliesGroup");
        repliesContainer.setAutoStartup(false);
        return repliesContainer;
    }
    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate(ProducerFactory<String, String> pf,
                      ConcurrentMessageListenerContainer<String, String> repliesContainer) {
        return new ReplyingKafkaTemplate<>(pf,repliesContainer);
    }
    @Bean
    public KafkaTemplate kafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate(pf);
    }

    //应用启动时就创建主题
    @Bean
    public NewTopic topic2() {
        return new NewTopic("topic-default", 1, (short) 1);
    }
}
