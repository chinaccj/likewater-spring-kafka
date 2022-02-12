package com.example.kafkasample01;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

@SpringBootApplication
public class KafkaSample01Application {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSample01Application.class, args);
    }


    @KafkaListener(groupId = "testgroup1", topics = "topic1" )
    public void onMessage(String msg, Acknowledgment ack){

        System.out.println("get message from topic1  "+msg);

        ack.acknowledge();
    }
}
