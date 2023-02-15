package com.example.kafka.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class ProducerServiceImpl {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private KafkaTemplate<Object, Object> template;
//    @Autowired
//    private ReplyingKafkaTemplate<Object, Object, Object> replyingKafkaTemplate;


    //同步发送
    public void sendSync(String topic,Object data){
        ListenableFuture<SendResult<Object, Object>> future = template.send("tipic.demo1", "");
        try {
            SendResult<Object,Object> result = future.get();
        }catch (Throwable e){
            e.printStackTrace();
        }
    }
    //异步发送
    public void sendAsync(){
        template.send("tipic.demo1", "").addCallback(new ListenableFutureCallback<SendResult<Object, Object>>() {
            @Override
            public void onFailure(Throwable throwable) {
                //
            }

            @Override
            public void onSuccess(SendResult<Object, Object> objectObjectSendResult) {
                RecordMetadata recordMetadata = objectObjectSendResult.getRecordMetadata();
                String topic = recordMetadata.topic();
                int partition = recordMetadata.partition(); // 消息发送到的分区
                long offset = recordMetadata.offset();  // 消息在分区内的offset
                logger.info("sendAsync success: {}-{}-{}",topic,partition,offset);
                //
            }
        });
    }

    //发送消息的不同入参
    public void send(){

        //指定主题和分区、事件时间、数据key
        template.send("tipic.demo1",0,System.currentTimeMillis(), "key1", "message");
        //使用ProducerRecord发送消息 (Kafka提供的原生客户端类)
        ProducerRecord record = new ProducerRecord("topic.demo1", "message");
        template.send(record);
        //发送给默认Topic
        template.sendDefault(record);
    }
    //开始事务模式-方式1
    public void testTrans(){
        template.executeInTransaction(operations -> {
            operations.send("tipic.demo1","test executeInTransaction");
            throw new RuntimeException("fail");
        });
    }
    //开始事务模式-方式2 @Transactional注解
    @Transactional
    public void testTrans2(){
        template.send("tipic.demo1","test executeInTransaction");
        throw new RuntimeException("fail");
    }
    //事务模式
}
