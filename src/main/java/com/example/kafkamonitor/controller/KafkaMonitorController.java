package com.example.kafkamonitor.controller;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkamonitor.TopicsManager;
import com.example.kafkamonitor.model.KafkaConfig;
import com.example.kafkamonitor.model.Topic;
import com.example.kafkamonitor.model.Topics;

@RestController
public class KafkaMonitorController {
    
    @Autowired
    private TopicsManager manager;

    private static int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }
    
    private void AddTopics(TopicsManager manager, String kafkaInstance, int numTopics, int numMinutes) {
        manager.getAllTopics().putIfAbsent(kafkaInstance, new Topics(kafkaInstance, new KafkaConfig()));
        for (int i = 0; i < numTopics; i++) {
            Topic t = new Topic("index-" + i, new KafkaConfig());
            for (int minutes = numMinutes; minutes >= 0; minutes--) {
                t.AddCountForTime(Instant.now().minus(minutes, ChronoUnit.MINUTES), getRandomNumber(0, 100));
            }
            manager.getAllTopics().get(kafkaInstance).getTopics().put(t.getName(), t);
        }
    }

    @PutMapping("/topics")
    public void putTopics(@RequestParam(value = "instance") String instance, 
                          @RequestParam(value = "topics", defaultValue = "3") int numTopics, 
                          @RequestParam(value = "minutes", defaultValue = "5") int numMinutes) {
        AddTopics(manager, instance, numTopics, numMinutes);
    }

    @GetMapping("/topics")
    public Map<String, Topics> topics() {
        return manager.getAllTopics();
    }

    @GetMapping("/topics/{instance}")
    public Topics instanceTopics(@PathVariable String instance) {
        return manager.getInstanceTopics(instance);
    }

    @GetMapping("/topics/{instance}/names")
    public List<String> topicNames(@PathVariable String instance) throws Exception {
        return manager.getInstanceTopicNames(instance);
    }

    @GetMapping("/topics/{instance}/list")
    public List<Topic> topicList(@PathVariable String instance, @RequestParam(value = "name", defaultValue = "ALL") String topicString) throws Exception {
        return manager.getInstanceTopicsList(instance, topicString);
    }

    @GetMapping("/topics/{instance}/topic")
    public Topic topic(@PathVariable String instance, @RequestParam(value = "name") String topic) throws Exception {
        return manager.getInstanceTopicData(instance, topic);
    }

    @PostMapping("/topics/{instance}/refresh")
    public void refreshInstance(@PathVariable String instance) {
        manager.UpdateTopicList(instance);
    }

    @PostMapping("/topics/{instance}/start")
    public String startInstanceCount(@PathVariable String instance) {
        int num = manager.StartCounting(instance);
        return "{ \"result\": \"successfully started " + num + " topics counting.\"}";
    }

    @PostMapping("/topics/{instance}/stop")
    public String stopInstanceCount(@PathVariable String instance) throws InterruptedException {
        int num = manager.StopCounting(instance);
        return "{ \"result\": \"successfully stopped " + num + " topics counting.\"}";
    }

    @PutMapping("/config")
    public String putConfig(@RequestBody KafkaConfig config) {
        System.out.println("Bootstrap server: '" + config.getBootstrapServer() + "'");
       manager.SetNewConfig(config);
       return "{ \"result\": \"success\" }";
    }
}