package com.example.kafkasample04;

import com.example.kafkasample04.common.Foo1;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.RecoveringBatchErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.backoff.FixedBackOff;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class KafkaSample04Application {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSample04Application.class, args);

    }

    private Map<String, Object> batchConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //自动提交设置为false
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //一次拉取消息数量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    private Map<String, Object> singleConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //自动提交设置为false
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }



    private Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        // 指定多个kafka集群多个地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 键的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return props;
    }


    @Bean("batchContainerFactory")
    public ConcurrentKafkaListenerContainerFactory listenerContainer() {
        ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
        container.setConsumerFactory(new DefaultKafkaConsumerFactory(batchConsumerProps()));

        container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        //并发线程数。 集群的并发线程数不低于
        container.setConcurrency(1);
        //设置为批量监听
        container.setBatchListener(true);

        KafkaTemplate<String, Object> template = new KafkaTemplate<String, Object>(new DefaultKafkaProducerFactory<>(producerConfigs()));
        RecoveringBatchErrorHandler errorHandler =
                new RecoveringBatchErrorHandler(new DeadLetterPublishingRecoverer(template),new FixedBackOff(2000L, 2));
        container.setBatchErrorHandler(errorHandler);

        return container;
    }

    @Bean("singleContainerFactory")
    public ConcurrentKafkaListenerContainerFactory listenerSingleConsumerContainer() {
        ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
        container.setConsumerFactory(new DefaultKafkaConsumerFactory(singleConsumerProps()));

        container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        //并发线程数。 集群的并发线程数不低于
        container.setConcurrency(1);
        //设置为批量监听
        container.setBatchListener(false);

        return container;
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


    //

    // 批量拉取模式
    @KafkaListener(id = "fooGroup",groupId = "testgroup", topics = "topic1",containerFactory = "batchContainerFactory")
    public void onMessage1(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack){
        // 消费的哪个topic、partition的消息,打印出消息内容
        System.out.println(">>>批量消费一次，records.size()="+records.size());

        for (int i =0;i< records.size(); i++) {
            ConsumerRecord record = records.get(i);

            ObjectMapper mapper = new ObjectMapper();
            Foo1 foo = null;
            try {
                foo = mapper.readValue((byte[])record.value(), Foo1.class);
                System.out.println(foo.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }


            if(foo.getFoo().equals("fail")){
                throw new BatchListenerFailedException("error",i);
            }

        }

        ack.acknowledge();

    }




    @KafkaListener(id = "dltGroup", topics = "topic1.DLT",containerFactory = "singleContainerFactory")
    public void dltListen(ConsumerRecord<String, byte[]> in,Acknowledgment ack) {
        ObjectMapper mapper = new ObjectMapper();
        Foo1 foo = null;
        try {
            foo = mapper.readValue(in.value(), Foo1.class);
            System.out.println("Received from DLT: " + foo.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }

        ack.acknowledge();
    }
}
