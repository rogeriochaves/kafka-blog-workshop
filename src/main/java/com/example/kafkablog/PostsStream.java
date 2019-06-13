package com.example.kafkablog;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
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

        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<Post> postSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Post.class));
        final KStream<String, Post> postsStream = builder.stream("posts",
                Consumed.with(Serdes.String(), postSerde));

        postsStream.foreach((k, v) -> {
            System.out.println("Received item " + k + " " + v.getTitle());
        });

        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "posts-listener");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaStreams kafkaConsumer = new KafkaStreams(builder.build(), streamProps);
        kafkaConsumer.start();
    }

    public static PostsStream getInstance() {
        return singleton;
    }

    public void produce(Post post) {
        kafkaProducer.send("posts", post.getId(), post);
    }
}
