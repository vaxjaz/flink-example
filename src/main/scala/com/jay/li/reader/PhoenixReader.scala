package com.jay.li.reader

import java.sql.DriverManager.getConnection
import java.sql.{Connection, PreparedStatement}

import com.jay.li.entity.{FunnelReq, PhoenixEventDO}
import com.jay.li.util.DateUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class PhoenixReader(hbaseHost: String, funnelReq: FunnelReq) extends RichSourceFunction[PhoenixEventDO] {


  private val driverClassName = "org.apache.phoenix.jdbc.PhoenixDriver"

  private val url = "jdbc:phoenix:" + hbaseHost + ":2181"

  private var conn: Connection = _

  private var pstmt: PreparedStatement = _


  override def run(sourceContext: SourceFunction.SourceContext[PhoenixEventDO]): Unit = {
    val begin = DateUtils.anyDayBeforeTodayTimeStamp(funnelReq.duration)
    val end = DateUtils.nowDayTimeStamp()
    val sql = "select user_id,event_id,time from EVENT_LOG where time > " + begin + " and time <" + end
    pstmt = conn.prepareStatement(sql)
    val resultSet = pstmt.executeQuery()
    while (resultSet.next()) {
      val userId = resultSet.getString("user_id")
      val time = resultSet.getLong("time")
      val eventId = resultSet.getString("event_id")
      val event = new PhoenixEventDO()
      event.eventId = eventId
      event.time = time
      event.userId = userId
      sourceContext.collect(event)
    }
  }


  override def cancel(): Unit = {

  }

  override def open(parameters: Configuration): Unit = {
    Class.forName(driverClassName)
    conn = getConnection(url)
  }

  override def close(): Unit = {
    if (null != conn) conn.close()
    if (null != pstmt) pstmt.close()
  }
}
