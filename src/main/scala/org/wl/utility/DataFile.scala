package org.wl.utility

import org.wl.consumer.Consumer

import java.io.{File, PrintWriter}
import java.time.Duration
import java.util.Properties
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.util.control.Breaks
import scala.util.control.Breaks.break

object DataFile {
  val writer = createLocal

  /**
   * write all data from kafka topic to a file
   *
   @param topic: name of the topic
   @param consumer : com.msl.kafkaui.consumer object of this library
   @param config: com.msl.kafkaui.consumer properties
   */
  def createDataFile(topic: String, consumer: Consumer, config: Properties) = {

    val kafkaConsumer = consumer.getConsumerFromStartOffset(topic)
    var count = 0
    var unchangedCount = 0
    var prevCount = 0

    Breaks.breakable {
      while (true) {
        val records = kafkaConsumer.poll(Duration.ofMillis(1000))
        if (!records.isEmpty) records.foreach { x =>
          prevCount = count
          count = count + 1
          val data = x.value()
          write(writer, data)
        }
        else prevCount = count

        if (count == prevCount) unchangedCount = unchangedCount + 1
        else unchangedCount = 0
        if (unchangedCount > 30) {
          writer.close()
          break
        }
      }
    }
    println("DONE!!")
    println("total records count: " + count)
  }

  def createLocal = {
    val dir = "/home/bizviz-admin/Documents/temp/"
    val writer = new PrintWriter(new File(dir + "sample.json"))
    writer
  }

  def write(writer: PrintWriter, data: String) = {
    writer.write(data + "\n")
  }
}
