package org.wl.utility

import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsResult, NewPartitions, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.TopicExistsException

import java.util
import java.util.concurrent.ExecutionException
import java.util.{Collections, Properties}
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.util.{Failure, Success, Try}


object TopicUtils {

  def getNumberOfPartitions(topic: String, config: Properties) = {
    val eventConsumer = new KafkaConsumer[String, String](config)
    eventConsumer.partitionsFor(topic).size
  }

  def increasePartitions(topic: String, adminClient: AdminClient, partitionCount: Int) = {
    val map = new util.HashMap[String, NewPartitions]()
    val np = NewPartitions.increaseTo(partitionCount)
    map.put(topic, np)
    adminClient.createPartitions(map)
  }

  def createTopic(topic: String, adminClient: AdminClient, partitionCount: Int = 1, rf: Int = 1) = {
    val env = sys.env
    try {
      try {
        val newTopic: NewTopic = new NewTopic(topic, partitionCount, rf.toShort)
        val topics = Collections.singleton(newTopic)
        val createTopicsResult: CreateTopicsResult = adminClient.createTopics(topics)
        createTopicsResult.values.get(topic).get
        Thread.sleep(1000)
      } catch {
        case e@(_: InterruptedException | _: ExecutionException) => if (!e.getCause.isInstanceOf[TopicExistsException]) {
          throw new RuntimeException(e.getMessage, e)
        }
      }
    } finally {
      if (adminClient != null) adminClient.close()
    }
  }

  def deleteTopic(topic: String, adminClient: AdminClient): Boolean = {
    Try {
      val res = adminClient.deleteTopics(util.Arrays.asList(topic))
      while (!res.all().isDone) {
        Thread.sleep(500)
      }
      true
    } match {
      case Success(value) => value
      case Failure(exception) => false
    }
  }

  def getAllTopics(config: Properties): util.Set[String] = {
    val adminClient: AdminClient = AdminClient.create(config)
    adminClient.listTopics.names.get
  }

  def checkIfTopicExist(adminClient: AdminClient, topic: String): Boolean = {
    adminClient.listTopics().names().get().contains(topic)
  }

  def getTopicInfo(adminClient: AdminClient, topic: String) = {
    val desc = adminClient.describeTopics(java.util.Arrays.asList(topic))
    println(desc.all().get().values())
  }

}
