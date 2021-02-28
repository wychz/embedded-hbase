package com.iiichz.embeddedhbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.executor.ExecutorService

class EmbeddedHConnection(embeddedHBaseHBase: EmbeddedHBase, conf: Configuration = HBaseConfiguration.create()) extends FakeTypes {
  private val mEmbeddedHBase: EmbeddedHBase = embeddedHBaseHBase
  private val mConf: Configuration = conf

  var closed: Boolean = false
  var aborted: Boolean = false

  def close() {
    closed = true
  }

  def abort(): Unit = {
    aborted = true
  }

  def isClosed(): Boolean = closed

  def isAborted(): Boolean = aborted

  // -----------------------------------------------------------------------------------------------
  // getTable() and aliases:

  def getTable(name: Bytes): org.apache.hadoop.hbase.client.Table = {
    getTable(TableName.valueOf(name), null)
  }

  def getTable(name: Bytes, pool: ExecutorService): org.apache.hadoop.hbase.client.Table = {
    getTable(TableName.valueOf(name), pool)
  }

  def getTable(name: String): org.apache.hadoop.hbase.client.Table = {
    getTable(TableName.valueOf(name), null)
  }

  def getTable(name: String, pool: ExecutorService): org.apache.hadoop.hbase.client.Table = {
    getTable(TableName.valueOf(name), pool)
  }

  def getTable(name: TableName): org.apache.hadoop.hbase.client.Table = {
    getTable(name, null)
  }

  def getTable(name: TableName, pool: ExecutorService): org.apache.hadoop.hbase.client.Table = {
    val fullName = name.getNameAsString()  // namespace:table-name
    mEmbeddedHBase.InterfaceFactory.create(mConf, fullName)
  }

}
