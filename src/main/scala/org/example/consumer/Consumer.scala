package org.example.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.example.utility.{TopicUtils, Utils}

import scala.collection.JavaConverters.asJavaCollectionConverter

class Consumer {

  def getConsumer(topic: String): KafkaConsumer[String, String] = {

    val consumer = new KafkaConsumer[String, String](new Utils().getKafkaProps())
    consumer.subscribe(java.util.Arrays.asList(topic))
    consumer
  }

  def getConsumerFromStartOffset(topic: String): KafkaConsumer[String, String] = {
    val config = new Utils().getKafkaProps
    val consumer = new KafkaConsumer[String, String](config)
    val numberOfPartitions = TopicUtils.getNumberOfPartitions(topic, config)
    val topicPartition = (0 until numberOfPartitions).toList.map(x => new TopicPartition(topic, x))
    consumer.assign(topicPartition.asJavaCollection)
    consumer.seekToBeginning(topicPartition.asJavaCollection)
    consumer
  }

  def getConsumerFromSpecifiedOffset(topic: String, offsets: Map[Int, Long]): KafkaConsumer[String, String] = {
    val config = new Utils().getKafkaProps
    val consumer = new KafkaConsumer[String, String](config)
    val numberOfPartitions = TopicUtils.getNumberOfPartitions(topic, config)
    for (i <- 0 until numberOfPartitions) {
      val topicPartition = new TopicPartition(topic, i)
      val offset = offsets.getOrElse(i, 0l)
      consumer.seek(topicPartition, offset)
    }
    consumer
  }
}
