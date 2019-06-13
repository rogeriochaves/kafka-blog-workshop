package com.example.kafkablog;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.*;

public class PostsStream {

    private static PostsStream singleton = new PostsStream();

    private static KafkaTemplate<String, Post> kafkaProducer;
    private static KafkaStreams kafkaConsumer;

    private PostsStream() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        this.kafkaProducer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));

        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<Post> postSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Post.class));
        builder.globalTable("posts", Consumed.with(Serdes.String(), postSerde), Materialized.as("postsTable"));

        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "posts-listener");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.kafkaConsumer = new KafkaStreams(builder.build(), streamProps);
        this.kafkaConsumer.start();
    }

    public static PostsStream getInstance() {
        return singleton;
    }

    public void produce(Post post) {
        kafkaProducer.send("posts", post.getId(), post);
    }

    public List<Post> findAll() {
        ReadOnlyKeyValueStore<String, Post> view = this.kafkaConsumer.store("postsTable", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, Post> iterator = view.all();

        List<Post> posts = new ArrayList<>();
        while (iterator.hasNext()) {
            posts.add(iterator.next().value);
        }
        return posts;
    }
}
