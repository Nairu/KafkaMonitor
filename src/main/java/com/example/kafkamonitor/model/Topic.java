package com.example.kafkamonitor.model;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.example.kafkamonitor.utils.KafkaUtils;
import com.example.kafkamonitor.utils.MutableInt;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.w3c.dom.NameList;

import ch.qos.logback.core.util.Duration;

public class Topic {
    private final String topicName;
    private final Map<Instant, MutableInt> topicCountByTime;
    private final KafkaConfig config;
    private final Consumer<String, String> consumer;
    private Thread consumerThread;

    private final AtomicBoolean keepRunning = new AtomicBoolean(false);

    public Topic(final String name, KafkaConfig config) {
        this.topicName = name;
        this.topicCountByTime = new HashMap<Instant, MutableInt>();
        this.config = config;
        consumer = KafkaUtils.createConsumer(topicName, config);
    }

    public synchronized void AddCountForTime(final Instant time, final int count) {
        final Instant minuteTruncateTime = time.truncatedTo(ChronoUnit.MINUTES);
        final MutableInt val = topicCountByTime.get(minuteTruncateTime);
        if (val == null) {
            topicCountByTime.put(minuteTruncateTime, new MutableInt(count));
        } else {
            val.increase(count);
        }
    }

    public String getName() {
        return this.topicName;
    }

    public Map<Instant, MutableInt> getTimes() {
        return this.topicCountByTime;
    }

    public void Start() {
        keepRunning.set(true);

        consumerThread = new Thread(consume());
        consumerThread.start();
    }

    public void Stop() throws InterruptedException {
        keepRunning.set(false);
        consumerThread.join();
    }

    public Runnable consume() {
        Runnable runnable = () -> {
            while (keepRunning.get()) {
                final ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofSeconds(1));
                if (records.count() > 0)
                    AddCountForTime(Instant.now(), records.count());
                consumer.commitAsync();
                // No need to spam it.
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // Failed!
                    break;
                }
            }
        };
        return runnable;
    }
}