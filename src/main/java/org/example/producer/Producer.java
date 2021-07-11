package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    public KafkaProducer<String, String> getProducer(Properties config) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        return producer;
    }

    public void sendMessage(String topic, String message, KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> msgToProduce = new ProducerRecord(topic, message);
        producer.send(msgToProduce);

    }

    public void sendMessageUsingKey(String topic, String message, KafkaProducer<String, String> producer, String key) {
        ProducerRecord<String, String> msgToProduce = new ProducerRecord(topic, message, key);
        producer.send(msgToProduce);

    }
}
