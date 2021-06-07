package com.example.kafkamonitor;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.example.kafkamonitor.model.Topic;
import com.example.kafkamonitor.model.Topics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaMonitorApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(KafkaMonitorApplication.class);
        app.run(args);
    }

}
