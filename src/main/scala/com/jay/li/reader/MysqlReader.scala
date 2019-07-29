package com.jay.li.reader

import java.sql.DriverManager.getConnection
import java.sql.{Connection, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MysqlReader(mysqlHost: String) extends RichSourceFunction[String] {

  private val driverClassName = "com.mysql.cj.jdbc.Driver"

  private val url = "jdbc:phoenix:" + mysqlHost + ":3306"

  private var conn: Connection = _

  private var pstmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    Class.forName(driverClassName)
    conn = getConnection(url)
  }

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    //do select
    val result = "test"
    sourceContext.collect(result)
  }

  override def cancel(): Unit = {
  }

  override def close(): Unit = {
    if (null != conn) conn.close()
    if (null != pstmt) pstmt.close()
  }
}
