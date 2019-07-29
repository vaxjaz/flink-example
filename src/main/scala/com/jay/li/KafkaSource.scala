package com.jay.li

import com.jay.li.util.KafkaUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object KafkaSource {

  def main(args: Array[String]): Unit = {
    val kafkaHost = if (args.isEmpty) "kafka" else args(0)
    val zookeeperHost = if (args.length < 2) "zookeeper" else args(1)
    val Array(kafkaServer, zookeeper, topics, groupId) = Array(kafkaHost + ":9092", zookeeperHost + ":2181", "xxx_topic", "xxx_group")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 读取kafka中的数据
    env.addSource(KafkaUtils.createKafkaInputStream(kafkaServer, zookeeper, topics, groupId))
      .filter(_.nonEmpty)
      // sink2kafka
      .addSink(KafkaUtils.sink2Kafka(kafkaHost + ":9092", topics))
  }

}
