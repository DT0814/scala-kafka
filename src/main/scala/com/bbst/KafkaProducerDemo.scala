package com.bbst

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties

object KafkaProducerDemo {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](prop)

    for (i <- 1 to 100) {
      val msg = s"${i}: this is a demo-topic ${i} kafka data"
      val rmd: RecordMetadata = producer.send(new ProducerRecord[String, String]("demo-topic", msg)).get()
      println(rmd.toString)
      println(s"send -->$msg successfully")
      Thread.sleep(500)
    }

    producer.close()
  }
}
