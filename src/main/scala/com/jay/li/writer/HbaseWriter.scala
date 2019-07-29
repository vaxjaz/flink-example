package com.jay.li.writer

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class HbaseWriter(tableName: String, hbaseHost: String) extends RichSinkFunction[String] {

  var conn: Connection = _
  var mutator: BufferedMutator = _
  var scan: Scan = _
  var count = 0

  /**
    * 建立HBase连接
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
    config.set(HConstants.ZOOKEEPER_QUORUM, hbaseHost)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)

    val name: TableName = TableName.valueOf(tableName)
    val params: BufferedMutatorParams = new BufferedMutatorParams(name)
    //设置缓存1m，当达到1m时数据会自动刷到hbase
    params.writeBufferSize(1024 * 1024) //设置缓存的大小
    mutator = conn.getBufferedMutator(params)
    count = 0
  }

  /**
    * 处理获取的hbase数据
    *
    * @param value
    * @param context
    */
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val cf1 = "cf1"
    val array: Array[String] = value.split(",")
    val put: Put = new Put(Bytes.toBytes(array(0)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array(2)))
    mutator.mutate(put)
    //每满2000条刷新一下数据
    if (count >= 2000) {
      mutator.flush()
      count = 0
    }
    count += 1
  }

  /**
    * 关闭
    */
  override def close(): Unit = {
    if (conn != null) conn.close()
  }

}
