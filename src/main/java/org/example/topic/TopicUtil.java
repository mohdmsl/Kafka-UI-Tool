package org.example.topic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TopicUtil {
    public TopicUtil() {
    }

    public Set<String> getAllTopics(Properties config) throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(config);
        return adminClient.listTopics().names().get();
    }

    public boolean deleteTopic(String topic, AdminClient adminClient) {

        List<String> topics = new ArrayList<String>();
        topics.add(topic);

        DeleteTopicsResult res = adminClient.deleteTopics(topics);
        while (!res.all().isDone()) {
        }
        return true;
    }

    public int getNumberOfPartitions(String topic, Properties config) {
        KafkaConsumer<String, String> eventConsumer = new KafkaConsumer<>(config);
        return eventConsumer.partitionsFor(topic).size();
    }
}
