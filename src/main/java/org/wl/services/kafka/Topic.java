package org.wl.services.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.wl.Alerts;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Topic {
    AdminClient adminClient;

    public Topic() {

    }

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
            new Alerts().createInformation(e, "Timeout while connecting to kafka server");
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

    public int getNumberOfPartitions(String topic, KafkaConsumer<String, String> eventConsumer) {
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

    public Offsets getOffsets(String topic, KafkaConsumer<String, String> consumer) {
        int numberOfPartitions = getNumberOfPartitions(topic, consumer);
        List<TopicPartition> partitions = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            partitions.add(new TopicPartition(topic, i));
        }
        Map<Integer, Long> endOffsets = consumer.endOffsets(partitions)
                .entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey().partition(),
                        entry -> entry.getValue()
                ));
        Map<Integer, Long> beginningOffsets = consumer.beginningOffsets(partitions)
                .entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey().partition(),
                        entry -> entry.getValue()
                ));
        return new Offsets(beginningOffsets, endOffsets);
    }


}
