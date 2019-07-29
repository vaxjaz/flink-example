package com.jay.li

import com.jay.li.entity.FunnelReq
import com.jay.li.reader.PhoenixReader
import com.jay.li.task.FunnelTask
import com.jay.li.util.GsonUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object PhoenixSource {

  def main(args: Array[String]): Unit = {
    // 从任务启动main函数中传入任务需要参数
    val mysqlHost = if (args.isEmpty) "mysql" else args(0)
    val hbaseHost = if (args.isEmpty) "hbase" else args(1)
    val funnelReqStr = if (args.isEmpty) "{\"duration\":90,\"steps\":[\"A\",\"B\"]}" else args(2)
    val funnelReq = GsonUtils.jsonStr2Class(funnelReqStr, classOf[FunnelReq])
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从自定义数据源中拉取数据(phoenix/mysql/hbase/...)
    val phoenixSource = env.addSource(new PhoenixReader(hbaseHost, funnelReq))
    FunnelTask.calculate(phoenixSource, funnelReq)
      .map(item => "name=" + item._1 + "  value=" + item._2)
      .print()
    env.execute()
  }

}
