package com.iiichz.embeddedhbase

import org.apache.hadoop.hbase.client.{Delete, Get}
import org.apache.hadoop.hbase.{HColumnDescriptor, HConstants, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.junit.{Assert, Test}
import org.slf4j.LoggerFactory

class TestEmbeddedHTable {
  private final val Log = LoggerFactory.getLogger(classOf[TestEmbeddedHTable])

  type Bytes = Array[Byte]

  implicit def stringToBytes(string: String): Bytes = {
    return Bytes.toBytes(string)
  }

  def bytesToString(bytes: Bytes): String = {
    return Bytes.toString(bytes)
  }

  class StringAsBytes(str: String) {
    def bytes(): Bytes = stringToBytes(str)

    def b = bytes()
  }

  implicit def implicitToStringAsBytes(str: String): StringAsBytes = {
    return new StringAsBytes(str)
  }

  def defaultTableDesc(): HTableDescriptor = {
    val desc = new HTableDescriptor(TableName.valueOf("table"))
    desc.addFamily(new HColumnDescriptor("table")
      .setMaxVersions(HConstants.ALL_VERSIONS)
      .setMinVersions(0)
      .setTimeToLive(HConstants.FOREVER)
      .setInMemory(false)
    )
    desc
  }

  def makeTableDesc(nfamilies: Int): HTableDescriptor = {
    val desc = new HTableDescriptor(TableName.valueOf("table"))
    for (ifamily <- 0 until nfamilies) {
      desc.addFamily(new HColumnDescriptor("family%s".format(ifamily))
        .setMaxVersions(HConstants.ALL_VERSIONS)
        .setMinVersions(0)
        .setTimeToLive(HConstants.FOREVER)
        .setInMemory(false)
      )
    }
    desc
  }

  @Test
  def testGetUnknownRow(): Unit = {
    val table = new EmbeddedHTable(name = "table", desc = defaultTableDesc)
    Assert.assertEquals(true, table.get(new Get("key")).isEmpty)
  }

  @Test
  def testScanEmptyTable(): Unit = {
    val table = new EmbeddedHTable(name = "table", desc = defaultTableDesc)
    Assert.assertEquals(null, table.getScanner("family").next)
  }

  @Test
  def testDeleteUnknownRow(): Unit = {
    val table = new EmbeddedHTable(name = "table", desc = defaultTableDesc)
    table.delete(new Delete("key"))
  }

  @Test
  def testPutThenGet(): Unit = {
    val table = new EmbeddedHTable(name = "table", desc = defaultTableDesc)
    table.put(new Put("key")
      .addColumn("family", "qualifier", 12345L, "value"))

    val result = table.get(new Get("key"))
    Assert.assertEquals(false, result.isEmpty)
    Assert.assertEquals("key", Bytes.toString(result.getRow))
    Assert.assertEquals("value", Bytes.toString(result.value))

    Assert.assertEquals(1, result.getMap.size)
    Assert.assertEquals(1, result.getMap.get("family"b).size)
    Assert.assertEquals(1, result.getMap.get("family"b).get("qualifier"b).size)
    Assert.assertEquals(
      "value",
      Bytes.toString(result.getMap.get("family"b).get("qualifier"b).get(12345L)))
  }
}
