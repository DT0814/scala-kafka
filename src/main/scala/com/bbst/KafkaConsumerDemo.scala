package com.bbst

import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.time.Duration

object KafkaConsumerDemo {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("group.id", "group01")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("enable.auto.commit", "true")
    val kafkaConsumer = new KafkaConsumer[String, String](prop)
    kafkaConsumer.subscribe(Collections.singletonList("demo-topic"))

    while (true) {
      val msgs: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofSeconds(100))
      val it = msgs.iterator()
      while (it.hasNext) {
        val msg = it.next()
        println(s"partition: ${msg.partition()}, offset: ${msg.offset()}, key: ${msg.key()}, value: ${msg.value()}")
      }
    }
  }
}