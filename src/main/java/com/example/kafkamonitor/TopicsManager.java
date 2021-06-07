package com.example.kafkamonitor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.example.kafkamonitor.controller.KafkaMonitorController;
import com.example.kafkamonitor.model.KafkaConfig;
import com.example.kafkamonitor.model.Topic;
import com.example.kafkamonitor.model.Topics;
import com.example.kafkamonitor.utils.KafkaUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component
public class TopicsManager {
    private Map<String, Topics> topics;
    private KafkaConfig config;
    
    public void LoadConfig(String filePath) throws JsonParseException, JsonMappingException, IOException {
        File file = new File(filePath);
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        config = om.readValue(file, KafkaConfig.class);
    }

    public void SetNewConfig(KafkaConfig newConfig) {
        config = newConfig;
        topics.putIfAbsent(newConfig.getBootstrapServer(), new Topics(newConfig.getBootstrapServer(), newConfig));
        UpdateTopicList(newConfig.getBootstrapServer());
    }

    public TopicsManager() {
        // Load the config from the yaml path.
        // loadConfig(yamlConfigPath)
        topics = new HashMap<String, Topics>();
    }

    public void UpdateTopicList(String instance) {
        System.out.println("Updating topics for instance: '" + instance + "'");
        Map<String, List<PartitionInfo>> info = KafkaUtils.getTopics(config);
        for (String topic : info.keySet()) {
            if (!config.getIgnoreTopics().contains(topic)) {
                if (!topics.get(instance).getTopics().containsKey(topic)) {
                    Topic t = new Topic(topic, config);
                    topics.get(instance).getTopics().put(topic, t);
                }
            }
        }
    }

    public void UpdateTopicList() {
        for (String kafkaInstance : new String[]{ "localhost:9092", "localhost:9093" }) {
            Map<String, List<PartitionInfo>> info = KafkaUtils.getTopics(config);
            for (String topic : info.keySet()) {
                if (!topics.get(kafkaInstance).getTopics().containsKey(topic)) {
                    Topic t = new Topic(topic, config);
                    topics.get(kafkaInstance).getTopics().put(topic, t);
                }
            }
        }
    }

    public int StartCounting(String instance) {
        int numTopicsStarted = 0;
        if (topics.containsKey(instance)) {
            topics.get(instance).Start();
            numTopicsStarted = topics.get(instance).getTopics().size();
        }
        return numTopicsStarted;
    }

    public int StopCounting(String instance) throws InterruptedException {
        int numTopicsStopped = 0;
        if (topics.containsKey(instance)) {
            topics.get(instance).Stop();
            numTopicsStopped = topics.get(instance).getTopics().size();
        }
        return numTopicsStopped;
    }

    public void StartCounting() {
        topics.values().forEach(t -> t.Start());
    }

    public void StopCounting() throws InterruptedException {
        for (Topics t : topics.values()) {
            t.Stop();
        }
    }

    public Map<String, Topics> getAllTopics() {
        return topics;
    }

    public Topics getInstanceTopics(String instance) {
        return topics.get(instance);
    }

    public List<String> getInstanceTopicNames(String instance) {
        return topics.get(instance).getTopics().keySet().stream().collect(Collectors.toList());
    }

    public List<Topic> getInstanceTopicsList(String instance, String predicate) {
        if (predicate == null)
            return topics.get(instance).getTopicsList();
        else
            return topics.get(instance).getTopicsList(predicate);
    }

    public Topic getInstanceTopicData(String instance, String topic) {
        return topics.get(instance).getTopic(topic);
    }
}