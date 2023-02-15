package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Collection;

@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka(count = 2,ports = {9092,9093},brokerPropertiesLocation = "classpath:kafka.properties")
class KafkaJavaDemoApplicationTests {

	@Autowired
	private KafkaProperties properties;

	@Test
	void contextLoads() {
	}

	@Test
	public void testCreateToipc(){
		AdminClient client = AdminClient.create(properties.buildAdminProperties());
		if(client !=null){
			try {
				Collection<NewTopic> newTopics = new ArrayList<>(1);
				newTopics.add(new NewTopic("topic-kl",1,(short) 1));
				client.createTopics(newTopics);
			}catch (Throwable e){
				e.printStackTrace();
			}finally {
				client.close();
			}
		}
	}
}
