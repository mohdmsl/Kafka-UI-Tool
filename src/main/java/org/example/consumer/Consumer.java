package org.example.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.example.topic.Topic;
import org.example.utility.Utils;

import java.time.Duration;
import java.util.*;

public class Consumer {

    AdminClient adminClient;

    public Consumer(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public KafkaConsumer<String, String> getConsumer(String topic) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(new Utils().getKafkaProps());
        consumer.subscribe(java.util.Arrays.asList(topic));
        return consumer;
    }

    public KafkaConsumer<String, String> getConsumerFromStartOffset(String topic) {
        Properties config = new Utils().getKafkaProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        int numberOfPartitions = new Topic(this.adminClient).getNumberOfPartitions(topic, config);
        List<TopicPartition> list = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            list.add(new TopicPartition(topic, i));
        }
        consumer.assign(list);
        consumer.seekToBeginning(list);
        return consumer;
    }

    public KafkaConsumer<String, String> getConsumerFromSpecifiedOffset(String topic, Map<TopicPartition, Long> offsets) {
        Properties config = new Utils().getKafkaProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (!offsets.isEmpty()) {
                    offsets.forEach(consumer::seek);
                } else {
                    consumer.seekToBeginning(partitions);
                }
            }
        });
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        ConsumerRecords<String, String> recordTemp = consumer.poll(Duration.ofMillis(100));
        System.out.println(recordTemp.isEmpty());
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            consumer.seek(new TopicPartition(topic, entry.getKey().partition()), entry.getValue());
        }
        return consumer;
    }
}
