package com.example.kafkasample05;

import com.example.kafkasample05.common.Foo1;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class KafkaSample05Application {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSample05Application.class, args);
    }

    @Bean
    public NewTopic topic() {
        return new NewTopic("topic1", 1, (short) 1);
    }

    //创建死信队列
    @Bean
    public NewTopic dlt() {
        return new NewTopic("topic1.DLT", 1, (short) 1);
    }

    private Map<String, Object> singleConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        //服务器列表
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //自动提交设置为false
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //设置consumer的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    @Bean("singleContainerFactory")
    public ConcurrentKafkaListenerContainerFactory listenerSingleConsumerContainer() {
        ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
        container.setConsumerFactory(new DefaultKafkaConsumerFactory(singleConsumerProps()));

        //设置消息确认消息模式为手动模式
        container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        //并发线程数。 集群的并发线程数不低于
        container.setConcurrency(1);
        //设置为非批量消费模式
        container.setBatchListener(false);

        return container;
    }


    @RetryableTopic(attempts = "5",backoff = @Backoff(delay = 2_000, maxDelay = 10_000, multiplier = 2), fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC)
    @KafkaListener(groupId = "testgroup", topics = "topic1",containerFactory= "singleContainerFactory")
    public void onMessageNormal(ConsumerRecord<String, byte[]> record, Acknowledgment ack){
        ObjectMapper mapper = new ObjectMapper();
        Foo1 foo = null;
        try {
            foo = mapper.readValue((byte[])record.value(), Foo1.class);
            System.out.println("msg content: "+foo.toString());

            if(foo.getFoo().equals("fail")){
                throw new RuntimeException("message consuemr error");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        ack.acknowledge();
    }

    @DltHandler
    public void processMessage(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        ObjectMapper mapper = new ObjectMapper();
        Foo1 foo = null;
        try {
            foo = mapper.readValue(record.value(), Foo1.class);
            System.out.println("Received from DLT: " + foo.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }

        ack.acknowledge();
    }
}
