package org.example.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

class Producer {


  def getProducer(config: Properties): KafkaProducer[String, String] = {
    val producer = new KafkaProducer[String, String](config)
    producer
  }

  def sendMessage(topic: String, message: String, producer: KafkaProducer[String, String]) = {
    try {
      val msgToProduce = new ProducerRecord[String, String](topic, message)
      producer.send(msgToProduce)
    } catch {
      case e: Exception =>
        throw e
    }
  }

  def sendMessageUsingKey(producer: KafkaProducer[String, String], msg: String, topic: String, key: String) = {
    try {
      val msgToProduce = new ProducerRecord[String, String](topic, key, msg)
      producer.send(msgToProduce)
    } catch {
      case e: Exception =>
        throw e
    }
  }
}
