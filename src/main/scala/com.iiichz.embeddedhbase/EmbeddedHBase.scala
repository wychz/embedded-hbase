package com.iiichz.embeddedhbase

import java.lang.{Integer => JInteger}
import java.util.Arrays
import java.util.{List => JList}
import java.util.{TreeMap => JTreeMap}

import com.iiichz.schema.impl.{HBaseAdminFactory, HBaseInterface, HTableInterfaceFactory}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.Buffer
import scala.math.BigInt.int2bigInt
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HRegionInfo, HTableDescriptor, TableExistsException, TableName, TableNotDisabledException, TableNotFoundException}
import org.apache.hadoop.hbase.client.{Connection, HBaseAdmin, HTable, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Pair

class EmbeddedHBase extends HBaseInterface with FakeTypes {
  private var createUnknownTable: Boolean = false
  private val mEmbeddedHConnection: EmbeddedHConnection = new EmbeddedHConnection(this)
  private val mHConnection: Connection = UntypedProxy.create(classOf[Connection], mEmbeddedHConnection)

  private[embeddedhbase] val tableMap = new JTreeMap[Bytes, EmbeddedHTable](Bytes.BYTES_COMPARATOR)

  def setCreateUnknownTable(createUnknownTableFlag: Boolean): Unit = {
    synchronized {
      this.createUnknownTable = createUnknownTableFlag
    }
  }

  object InterfaceFactory extends HTableInterfaceFactory {


    override def create(conf: Configuration, tableName: String): org.apache.hadoop.hbase.client.Table = {
      val tableNameBytes = Bytes.toBytes(tableName)
      synchronized {
        var table = tableMap.get(tableNameBytes)
        if (table == null) {
          if (!createUnknownTable) {
            throw new TableNotFoundException(tableName)
          }
          val desc = new HTableDescriptor(TableName.valueOf(tableName))
          table = new EmbeddedHTable(
            name = tableName,
            conf = conf,
            desc = desc,
            hconnection = mEmbeddedHConnection
          )
          tableMap.put(tableNameBytes, table)
        }
        return UntypedProxy.create(classOf[HTable], table)
      }
    }
  }

  override def getHTableFactory(): HTableInterfaceFactory = InterfaceFactory

  // -----------------------------------------------------------------------------------------------

  object Admin extends HBaseAdminCore with HBaseAdminConversionHelpers {
    def addColumn(tableName: Bytes, column: HColumnDescriptor): Unit = {
      // TODO(taton) Implement metadata
      // For now, do nothing
    }

    def createTable(desc: HTableDescriptor, split: Array[Bytes]): Unit = {
      synchronized {
        if (tableMap.containsKey(Bytes.toBytes(desc.getNameAsString))) {
          throw new TableExistsException(desc.getNameAsString)
        }
        val table = new EmbeddedHTable(
          name = desc.getNameAsString,
          desc = desc,
          hconnection = mEmbeddedHConnection
        )
        Arrays.sort(split, Bytes.BYTES_COMPARATOR)
        table.setSplit(split)
        tableMap.put(Bytes.toBytes(desc.getNameAsString), table)
      }
    }

    def createTable(
                     desc: HTableDescriptor,
                     startKey: Bytes,
                     endKey: Bytes,
                     numRegions: Int
                   ): Unit = {
      // TODO Handle startKey/endKey
      val split = Buffer[Bytes]()
      val min = 0
      val max: BigInt = (BigInt(1) << 128) - 1
      for (n <- 1 until numRegions) {
        val boundary: Bytes = MD5Space(n, numRegions)
        split.append(boundary)
      }
      createTable(desc = desc, split = split.toArray)
    }

    def deleteColumn(tableName: Bytes, columnName: Bytes): Unit = {
      // TODO(taton) Implement metadata
      // For now, do nothing
    }

    def deleteTable(tableName: Bytes): Unit = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        if (table.enabled) {
          throw new TableNotDisabledException(tableName)
        }
        tableMap.remove(tableName)
      }
    }

    def disableTable(tableName: Bytes): Unit = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        table.enabled = false
      }
    }

    def enableTable(tableName: Bytes): Unit = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        table.enabled = true
      }
    }

    def flush(tableName: Bytes): Unit = {
      // Nothing to do
    }

    def getTableRegions(tableName: Bytes): JList[HRegionInfo] = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        return table.getRegions()
      }
    }

    def isTableAvailable(tableName: Bytes): Boolean = {
      return isTableEnabled(tableName)
    }

    def isTableEnabled(tableName: Bytes): Boolean = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        return table.enabled
      }
    }

    def listTables(): Array[HTableDescriptor] = {
      synchronized {
        return tableMap.values.iterator.asScala
          .map { table => table.getTableDescriptor }
          .toArray
      }
    }

    def modifyColumn(tableName: Bytes, column: HColumnDescriptor): Unit = {
      // TODO(taton) Implement metadata
    }

    def modifyTable(tableName: Bytes, desc: HTableDescriptor): Unit = {
      // TODO(taton) Implement metadata
    }

    def getAlterStatus(tableName: Bytes): Pair[JInteger, JInteger] = {
      return new Pair(0, getTableRegions(tableName).size)
    }

    def tableExists(tableName: Bytes): Boolean = {
      synchronized {
        return tableMap.containsKey(tableName)
      }
    }
  }

  object AdminFactory extends HBaseAdminFactory {
    override def create(conf: Configuration): HBaseAdmin = {
      return UntypedProxy.create(classOf[HBaseAdmin], Admin)
    }
  }

  def getHConnection(): Connection = {
    return mHConnection
  }

  override def getAdminFactory: HBaseAdminFactory = ???
}
