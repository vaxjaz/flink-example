package com.jay.li.reader

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}

class HBaseReader(tableName: String, cf: String) extends RichSourceFunction[(String, String)] {

  private var conn: Connection = _
  private var table: Table = _
  private var scan: Scan = _

  override def run(sourceContext: SourceFunction.SourceContext[(String, String)]): Unit = {
    val rs = table.getScanner(scan)
    val iterator = rs.iterator()
    while (iterator.hasNext) {
      val result = iterator.next()
      val rowKey = Bytes.toString(result.getRow)
      val sb: StringBuffer = new StringBuffer()
      import scala.collection.JavaConverters._
      val list = result.listCells()
      for (cell: Cell <- list.asScala) {
        val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        sb.append(value).append("_")
      }
      val valueString = sb.replace(sb.length() - 1, sb.length(), "").toString
      sourceContext.collect((rowKey, valueString))
    }
  }


  override def open(parameters: Configuration): Unit = {
    val config = HBaseConfiguration.create()
    config.set(HConstants.ZOOKEEPER_QUORUM, "hbase")
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)
    table = conn.getTable(TableName.valueOf(tableName))
    scan = new Scan()
    scan.withStartRow(Bytes.toBytes("1"))
    scan.withStopRow(Bytes.toBytes("5"))
    scan.addFamily(Bytes.toBytes(cf))
  }

  override def close(): Unit = {
    try {
      if (table != null) {
        table.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  override def cancel(): Unit = {

  }

}
