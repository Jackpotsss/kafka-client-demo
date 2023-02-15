package com.example.kafka.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class AdminServiceImpl {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    public AdminClient adminClient(){
        AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        return client;
    }

    public void createTopic() throws InterruptedException, ExecutionException{

        AdminClient client = adminClient();
        //创建主题Topic
        NewTopic newTopic = new NewTopic("testTopic", 3, (short) 1);
        List<NewTopic> newTopics = Arrays.asList(newTopic);
        CreateTopicsResult createTopicResult = client.createTopics(newTopics);

        //查询Topic
        ListTopicsResult listTopicsResult = client.listTopics();
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.forEach(topic-> System.out.println(topic.name()));
        //查询Topic的描述信息
        DescribeTopicsResult result = client.describeTopics(Arrays.asList("testTopic"));
        Map<String, TopicDescription> descriptionMap = result.allTopicNames().get();
        descriptionMap.forEach((key, value) ->
                System.out.println("topicName: " + key + ", desc: " + value));
        //查询Topic的配置信息
//        new
        ConfigResource testTopic = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic");
        DescribeConfigsResult describeConfigsResult = client.describeConfigs(Arrays.asList(testTopic));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        configResourceConfigMap.forEach((key, value) ->
                System.out.println("topicName: " + key + ", config: " + value));
        //创建分区
//        client.createPartitions()；

    }
    public void deleteTopics(){
        AdminClient client = adminClient();
        client.deleteTopics(Arrays.asList("testTopic")); //删除主题
    }
    /**
     * 增加Partition数量，目前Kafka不支持删除或减少Partition
     */
    public void incrPartitions() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        newPartitions.put("testTopic", NewPartitions.increaseTo(2)); // 将MyTopic的Partition数量调整为2
        CreatePartitionsResult result = adminClient.createPartitions(newPartitions);
        System.out.println(result.all().get());
    }
    public void test() throws ExecutionException, InterruptedException{
        AdminClient adminClient = adminClient();

        //删除消息记录
        Map<TopicPartition, RecordsToDelete> recordMap = new HashMap<>();
        TopicPartition topicPartition = new TopicPartition("testTopic", 0);
        RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(4321L);
        recordMap.put(topicPartition,recordsToDelete);
        adminClient.deleteRecords(recordMap);

        //查询所有的消费者分组
        adminClient.listConsumerGroups();

        /************* 偏移量相关操作 ************/
        //查询所有主题分区的偏移量信息
//        adminClient.listOffsets()
//        //查询所有的消费者分组偏移量
//        adminClient.listConsumerGroupOffsets()
//
//        //改变指定组的偏移量
//        adminClient.alterConsumerGroupOffsets();
//        /************* 事务相关操作 ************/
//        //查询所有的事务信息
//        adminClient.listTransactions();
//        //查询具体的事务详情
//        adminClient.describeTransactions()
//        //回滚事务
//        adminClient.abortTransaction()
//
//        /************* 权限相关操作 ************/
//        //创建绑定到指定资源的访问控制列表 (ACL)。
//        adminClient.createAcls()
//        //删除权限
//        adminClient.deleteAcls()
//        //查看权限
//        adminClient.describeAcls()
//
//        //修改分区副本的底层日志存储目录
//        adminClient.alterReplicaLogDirs()
//        //对kafka客户端进行限流
//        adminClient.alterClientQuotas()
//        //查看集群中节点的信息
//        adminClient.describeCluster()
    }



}
