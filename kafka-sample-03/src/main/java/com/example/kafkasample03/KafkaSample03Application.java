package com.example.kafkasample03;

import com.example.kafkasample03.common.Foo1;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.io.IOException;

@SpringBootApplication
public class KafkaSample03Application {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSample03Application.class, args);
    }

    @KafkaListener(groupId = "testgroup1", topics = "topic1" )
    public void onMessage(byte[] msg, Acknowledgment ack){

        ObjectMapper mapper = new ObjectMapper();
        Foo1 foo = null;
        try {
            foo = mapper.readValue(msg, Foo1.class);
            System.out.println("get message from topic1  "+foo.toString());

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(),e);
        }

        ack.acknowledge();
    }
}
