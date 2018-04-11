package com.robertsmieja.example.kafka.service;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@RequiredArgsConstructor
@Service
@ShellComponent
public class KafkaConsumerService {
    private final KafkaConfigService kafkaConfigService;
    private KafkaConsumer<String, String> kafkaConsumer;

    @ShellMethodAvailability
    public Availability availabilityCheck() {
        Properties configuration = kafkaConfigService.getConfiguration();
        if (configuration != null) {
            kafkaConsumer = new KafkaConsumer<>(configuration);
            return Availability.available();
        }
        else {
            return Availability.unavailable("Not configured");
        }
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

    @ShellMethod("List all Kafka topics")
    Map<String, List<PartitionInfo>> listTopics(){
        return kafkaConsumer.listTopics();
    }
}
