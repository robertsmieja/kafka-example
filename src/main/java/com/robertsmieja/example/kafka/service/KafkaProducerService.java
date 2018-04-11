package com.robertsmieja.example.kafka.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RequiredArgsConstructor
@Service
@ShellComponent
public class KafkaProducerService {
    private final KafkaService kafkaService;

    private String topic = "test";

    @ShellMethodAvailability
    public Availability availabilityCheck() {
        Properties configuration = kafkaService.getConfiguration();
        if (configuration != null) {
            return Availability.available();
        }
        else {
            return Availability.unavailable("Not configured");
        }
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
        Future<RecordMetadata> metadataFuture = kafkaService.getKafkaProducer().send(record);
        return metadataFuture.get();
    }
}
