package org.example.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.example.topic.TopicUtil;
import org.example.utility.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Consumer {

    public KafkaConsumer<String, String> getConsumer(String topic) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(new Utils().getKafkaProps());
        consumer.subscribe(java.util.Arrays.asList(topic));
        return consumer;
    }

    public KafkaConsumer<String, String> getConsumerFromStartOffset(String topic) {
        Properties config = new Utils().getKafkaProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(new Utils().getKafkaProps());
        int numberOfPartitions = new TopicUtil().getNumberOfPartitions(topic, config);
        List<TopicPartition> list = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            list.add(new TopicPartition(topic, i));
        }
        consumer.assign(list);
        consumer.seekToBeginning(list);
        return consumer;
    }

    public KafkaConsumer<String, String> getConsumerFromSpecifiedOffset(String topic, Map<Integer, Long> offsets) {
        Properties config = new Utils().getKafkaProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(new Utils().getKafkaProps());
        int numberOfPartitions = new TopicUtil().getNumberOfPartitions(topic, config);
        for (int i = 0; i < numberOfPartitions; i++) {
            TopicPartition topicPartition = new TopicPartition(topic, i);
            Long offset = offsets.getOrDefault(i, 0l);
            consumer.seek(topicPartition, offset);
        }
        return consumer;
    }
}
