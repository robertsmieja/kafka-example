package com.robertsmieja.example.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

@Service
@ShellComponent
public class KafkaConsumerService {
    private final KafkaConsumer<String, String> kafkaConsumer;

    KafkaConsumerService() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "consumerGroup");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer = new KafkaConsumer<>(props);
    }

    @ShellMethod("Subscribe to Kafka topics")
    void subscribe(@ShellOption(defaultValue = "test") List<String> topics) {
        kafkaConsumer.subscribe(topics);
    }

    @ShellMethod("Poll subscribed Kafka topics")
    ConsumerRecords<String, String> poll(@ShellOption(defaultValue = "100") long timeout) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(timeout);
        records.forEach(record -> System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
        return records;
    }

}
