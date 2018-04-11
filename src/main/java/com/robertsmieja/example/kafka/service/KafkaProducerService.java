package com.robertsmieja.example.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
@ShellComponent
public class KafkaProducerService {
    KafkaProducer<String, String> kafkaProducer;
    String topic = "test";

    public KafkaProducerService() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 2000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<>(props);
    }

    @ShellMethod("Get current Kafka topic")
    String getTopic() {
        return topic;
    }

    @ShellMethod("Set current Kafka topic")
    void setTopic(String topic) {
        this.topic = topic;
    }

    @ShellMethod("Send message to current Kafka topic")
    RecordMetadata send(@ShellOption(defaultValue = "foo") String key, @ShellOption(defaultValue = "bar") String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> metadataFuture = kafkaProducer.send(record);
        return metadataFuture.get();
    }
}
