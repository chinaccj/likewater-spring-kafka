package com.example.kafkasample03.controller;

/**
 * @Author: likewater
 * @Description:
 * @Date: Create in 8:58 下午 2022/2/12
 */
import com.example.kafkasample03.common.Foo1;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {
    @Autowired
    private KafkaTemplate<Object, Object> template;

    @PostMapping(path = "/send/foo/{what}")
    public void sendFoo(@PathVariable String what) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            byte [] msgBytes = mapper.writeValueAsBytes(new Foo1(what));
            this.template.send("topic1", msgBytes);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}