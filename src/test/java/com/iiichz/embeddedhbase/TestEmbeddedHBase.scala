package com.iiichz.embeddedhbase

import scala.collection.JavaConverters.asScalaBufferConverter
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HTable
import org.junit.Assert
import org.junit.Test

class TestEmbeddedHBase {

  @Test
  def testEmbeddedHBase(): Unit = {
    val hbase = new EmbeddedHBase()
    val desc = new HTableDescriptor(TableName.valueOf("table-name"))
    hbase.Admin.createTable(desc)

    val tables = hbase.Admin.listTables()
    Assert.assertEquals(1, tables.length)
    Assert.assertEquals("table-name", tables(0).getNameAsString())
  }

  @Test
  def testSimpleRegionSplit(): Unit = {
    val hbase = new EmbeddedHBase()
    val desc = new HTableDescriptor(TableName.valueOf("table-name"))
    hbase.Admin.createTable(desc, null, null, numRegions = 2)

    val regions = hbase.Admin.getTableRegions("table-name".getBytes).asScala
    Assert.assertEquals(2, regions.size)
    assert(regions.head.getStartKey.isEmpty)
    assert(regions.last.getEndKey.isEmpty)
    for (i <- 0 until regions.size - 1) {
      Assert.assertEquals(
        regions(i).getEndKey.toSeq,
        regions(i + 1).getStartKey.toSeq)
    }
    Assert.assertEquals(
      "7fffffffffffffffffffffffffffffff",
      Hex.encodeHexString(regions(0).getEndKey))
  }

  @Test
  def testEmbeddedHTableAsInstanceOfHTable(): Unit = {
    val hbase = new EmbeddedHBase()
    val desc = new HTableDescriptor(TableName.valueOf("table"))
    hbase.Admin.createTable(desc)
    val conf = HBaseConfiguration.create()
    val htable: HTable = hbase.InterfaceFactory.create(conf, "table").asInstanceOf[HTable]
  }

  @Test
  def testAdminFactory(): Unit = {
    val hbase = new EmbeddedHBase()
    val conf = HBaseConfiguration.create()
    val admin = hbase.AdminFactory.create(conf)
    admin.close()
  }
}

