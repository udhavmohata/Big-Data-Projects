package com.uv.kafka.producer.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class ProducerController
{
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/publish/{message}")
    public String Publish(@PathVariable("message") String message)
    {
        String TOPIC = "kafkaPublish";
        kafkaTemplate.send(TOPIC,message);
        return "Published Successfully";
    }
}
