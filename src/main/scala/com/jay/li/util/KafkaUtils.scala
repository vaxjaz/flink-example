package com.jay.li.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}

object KafkaUtils {

  def createKafkaInputStream(kafkaServer: String, zookeeper: String, topic: String, groupId: String): FlinkKafkaConsumer09[String] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaServer)
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", zookeeper)
    properties.setProperty("group.id", groupId)

    val kafkaConsumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema, properties)
    kafkaConsumer.setStartFromLatest()
    kafkaConsumer
  }

  def sink2Kafka(kafkaServer: String, topic: String): FlinkKafkaProducer09[String] = {
    val producer = new FlinkKafkaProducer09[String](
      kafkaServer,
      topic,
      new SimpleStringSchema)
    producer
  }
}
