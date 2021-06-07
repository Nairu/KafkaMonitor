package com.example.kafkamonitor.utils;

import java.util.Collections;
import java.util.Properties;

import com.example.kafkamonitor.model.KafkaConfig;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaUtils {

    private static Properties getProperties(KafkaConfig config) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    config.getBootstrapServer());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "kafka-metrics-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, config.isIgnoreInternalTopics());
        if (config.isStartFromBeginning()) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        
        return props;
    }

    public static Map<String, List<PartitionInfo>> getTopics(KafkaConfig config) {
        try (final Consumer<String, String> consumer =
                                    new KafkaConsumer<>(getProperties(config))) {
            return consumer.listTopics();
        }
    }

    public static Consumer<String, String> createConsumer(String topic, KafkaConfig config) {
        // Create the consumer using props.
        final Consumer<String, String> consumer =
                                    new KafkaConsumer<>(getProperties(config));
  
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}