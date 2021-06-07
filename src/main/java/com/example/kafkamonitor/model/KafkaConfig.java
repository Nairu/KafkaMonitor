package com.example.kafkamonitor.model;

import java.util.ArrayList;
import java.util.List;

public class KafkaConfig {
    private String bootstrapServer = "";
    private List<String> ignoreTopics = new ArrayList<String>();
    private boolean startFromBeginning = true;
    private boolean ignoreInternalTopics = true;

    public void setBootstrapServer(String servers) {
        this.bootstrapServer = servers;
    }

    public boolean isIgnoreInternalTopics() {
        return ignoreInternalTopics;
    }

    public void setIgnoreInternalTopics(boolean ignoreInternalTopics) {
        this.ignoreInternalTopics = ignoreInternalTopics;
    }

    public boolean isStartFromBeginning() {
        return startFromBeginning;
    }

    public void setStartFromBeginning(boolean startFromBeginning) {
        this.startFromBeginning = startFromBeginning;
    }

    public List<String> getIgnoreTopics() {
        return ignoreTopics;
    }

    public void setIgnoreTopics(List<String> ignoreTopics) {
        this.ignoreTopics = ignoreTopics;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }
}