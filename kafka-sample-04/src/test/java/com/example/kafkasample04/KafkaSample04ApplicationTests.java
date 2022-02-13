package com.example.kafkasample04;

import com.example.kafkasample04.common.Foo1;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

@SpringBootTest
class KafkaSample04ApplicationTests {
    @Autowired
    private KafkaTemplate<Object, Object> template;


    @Test
    void contextLoads() {
        template.send("topic1", new Foo1("hello1"));
        template.send("topic1", new Foo1("hello2"));
        template.send("topic1", new Foo1("fail"));
        template.send("topic1", new Foo1("hello3"));
        template.send("topic1", new Foo1("hello4"));
    }

}
