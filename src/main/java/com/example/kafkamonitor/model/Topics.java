package com.example.kafkamonitor.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Topics {
    private final String kafkaInstance;
    private final Map<String, Topic> topics;
    private final KafkaConfig config;

    public Topics(String kafkaInstanceInfo, KafkaConfig config) {
        this.kafkaInstance = kafkaInstanceInfo;
        this.topics = new HashMap<String, Topic>();
        this.config = config;
    }

    public String getInstance() {
        return kafkaInstance;
    }

    public Map<String, Topic> getTopics() {
        return topics;
    }

    @JsonIgnore
    public List<Topic> getTopicsList() {
        return topics.values().stream().collect(Collectors.toList());
    }

    @JsonIgnore
    public List<Topic> getTopicsList(String predicate) {
        return topics.values().stream().filter(x -> x.getName().startsWith(predicate)).collect(Collectors.toList());
    }

    @JsonIgnore
    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    public void Start() {
        topics.values().stream().forEach(c -> c.Start());
    }

    public void Stop() throws InterruptedException {
        for (Topic t : topics.values()) {
            t.Stop();
        }
    }
}