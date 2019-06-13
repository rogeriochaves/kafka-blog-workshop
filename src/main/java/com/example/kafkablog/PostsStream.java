package com.example.kafkablog;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.*;

public class PostsStream {

    private static PostsStream singleton = new PostsStream();

    private static KafkaTemplate<String, Post> kafkaProducer;

    private PostsStream() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        this.kafkaProducer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    public static PostsStream getInstance() {
        return singleton;
    }

    public void produce(Post post) {
        kafkaProducer.send("posts", post.getId(), post);
    }
}
