package com.robertsmieja.example.kafka.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;
import org.springframework.stereotype.Service;

import java.util.*;

@RequiredArgsConstructor
@Service
@ShellComponent
public class KafkaConsumerService {
    private final KafkaService kafkaService;

    @ShellMethodAvailability
    public Availability availabilityCheck() {
        Properties configuration = kafkaService.getConfiguration();
        if (configuration != null) {
            return Availability.available();
        } else {
            return Availability.unavailable("Not configured");
        }
    }

    @ShellMethod("Subscribe to Kafka topics")
    void subscribe(@ShellOption(defaultValue = "test") List<String> topics) {
        kafkaService.getKafkaConsumer().subscribe(topics);
    }

    @ShellMethod("Poll subscribed Kafka topics")
    ConsumerRecords<String, String> poll(@ShellOption(defaultValue = "100") long timeout) {
        ConsumerRecords<String, String> records = kafkaService.getKafkaConsumer().poll(timeout);
        records.forEach(record -> System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
        return records;
    }

    @ShellMethod("Return current Topic/Partition assignment")
    Set<TopicPartition> assignment(){
        return kafkaService.getKafkaConsumer().assignment();
    }

    @ShellMethod("Return beginning offsets for a Topic/Partition")
    Map<TopicPartition, Long> beginningOffsets(@ShellOption(defaultValue = "test")String topic, @ShellOption(defaultValue = "0") int partition) {
       return kafkaService.getKafkaConsumer().beginningOffsets(Collections.singleton(new TopicPartition(topic, partition)));
    }

    @ShellMethod("Seek to a given offset for a Topic/Partition")
    void seek(
            @ShellOption(defaultValue = "test")String topic,
            @ShellOption(defaultValue = "0") int partition,
            @ShellOption(defaultValue = "0") long offset
    ){
        kafkaService.getKafkaConsumer().seek(new TopicPartition(topic, partition), offset);
    }

    @ShellMethod("List all Kafka topics")
    Map<String, List<PartitionInfo>> listTopics() {
        return kafkaService.getKafkaConsumer().listTopics();
    }
}
