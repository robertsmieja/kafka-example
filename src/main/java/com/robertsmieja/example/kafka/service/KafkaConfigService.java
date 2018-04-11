package com.robertsmieja.example.kafka.service;


import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
@ShellComponent
public class KafkaConfigService {
    private Properties props;

    @ShellMethod("Configure Kafka")
    Properties configure(
            @ShellOption(defaultValue = "localhost:9092")String server,
            @ShellOption(defaultValue = "consumerGroup")String groupId
    ){
        Properties props = new Properties();
        //Common
        props.put("bootstrap.servers", server);
        props.put("group.id", groupId);


        //Consumer
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //Producer
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 2000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.props = props;
        return this.props;
    }

    @ShellMethod("Get current configuration")
    Properties getConfiguration(){
        return this.props;
    }
}
