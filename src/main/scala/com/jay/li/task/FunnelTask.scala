package com.jay.li.task

import java.util

import com.jay.li.entity.{FunnelReq, PhoenixEventDO}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.util.Collector

object FunnelTask {

  def calculate(dStream: DataStream[PhoenixEventDO], funnelReq: FunnelReq): DataStream[(String, Int)] = {
    val steps = funnelReq.steps
    var pattern = Pattern.begin[PhoenixEventDO]("step1")
      .where((value, ctx) => {
        value.eventId.equals(steps.get(0))
      })
      .followedBy("step2")
      .where((value, ctx) => {
        val preEvent = ctx.getEventsForPattern("step1").iterator
        var time = -1L
        while (preEvent.hasNext) {
          val pre = preEvent.next()
          time = pre.time
        }
        value.eventId.equals(steps.get(1)) && value.time > time
      })
    for (i <- 2 until steps.size()) {
      pattern = pattern.followedBy("step" + (i + 1))
        .where((value, ctx) => {
          val preEvent = ctx.getEventsForPattern("step1").iterator
          var time = -1L
          while (preEvent.hasNext) {
            val pre = preEvent.next()
            time = pre.time
          }
          value.eventId.equals(steps.get(i)) && value.time > time
        })
    }
    val result = CEP.pattern(dStream.keyBy(_.userId), pattern)
      .process(
        new PatternProcessFunction[PhoenixEventDO, (String, Int)]() {
          override def processMatch(map: util.Map[String, util.List[PhoenixEventDO]],
                                    context: PatternProcessFunction.Context,
                                    collector: Collector[(String, Int)]): Unit = {
            val userId = map.get("step1").get(0).userId
            collector.collect((userId, map.get("step1").size()))
          }
        }
      )
      .keyBy(_._1)
      .reduce((k1, k2) => {
        (k1._1, k1._2 + k2._2)
      })
    result
  }


  case class Count(name: String, result: Int)

}
