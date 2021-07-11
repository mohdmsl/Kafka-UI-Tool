package org.example.topic;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.example.utility.Utils;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Topic {
    AdminClient adminClient;

    public Topic(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public Set<String> getAllTopics() {
        Set<String> list = null;
        try {
            list = this.adminClient.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return list;
    }

    public boolean deleteTopic(String topic) {

        List<String> topics = new ArrayList<String>();
        topics.add(topic);

        DeleteTopicsResult res = this.adminClient.deleteTopics(topics);
        while (!res.all().isDone()) {
        }
        return true;
    }

    public int getNumberOfPartitions(String topic, Properties config) {
        KafkaConsumer<String, String> eventConsumer = new KafkaConsumer<>(config);
        return eventConsumer.partitionsFor(topic).size();
    }

    public boolean checkIfTopicExist(String topic, AdminClient adminClient) {
        boolean isExist = false;
        try {
            isExist = adminClient.listTopics().names().get().contains(topic);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return isExist;
    }

    public int getTotalTopics(Properties config) {
        AdminClient adminClient = AdminClient.create(config);
        int count = 0;
        try {
            count = adminClient.listTopics().names().get().size();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return count;
    }

    public int getNumberOfPartitionsInCluster() {

        int partitions = 0;
        try {
            Set<String> topics = getAllTopics();
            Iterator<TopicDescription> itr = adminClient.describeTopics(topics).all().get().values().iterator();
            while (itr.hasNext()) {
                partitions += itr.next().partitions().size();
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return partitions;
    }

    public void createTopic(String topic, int partitionCount, short rf) {
        NewTopic newTopic = new NewTopic(topic, partitionCount, rf);
        Set<NewTopic> topics = Collections.singleton(newTopic);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(topics);
    }

    public void createTopic(String topic) {
        int partitionCount = 1;
        short rf = 1;
        NewTopic newTopic = new NewTopic(topic, partitionCount, rf);
        Set<NewTopic> topics = Collections.singleton(newTopic);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(topics);
    }

    public Offsets getOffsets(String topic) {
        Properties config = new Utils().getKafkaProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        int numberOfPartitions = getNumberOfPartitions(topic, config);
        List<TopicPartition> partitions = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            partitions.add(new TopicPartition(topic, i));
        }
        return new Offsets(consumer.beginningOffsets(partitions), consumer.endOffsets(partitions));
    }
}
