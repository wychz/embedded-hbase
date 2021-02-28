package com.iiichz.embeddedhbase

import java.io.PrintStream
import java.lang.reflect.Field
import java.lang.{Boolean => JBoolean}
import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList}
import java.util.Arrays
import java.util.{Iterator => JIterator}
import java.util.{List => JList}
import java.util.{Map => JMap}
import java.util.NavigableMap
import java.util.NavigableSet
import java.util.{TreeMap => JTreeMap}
import java.util.{TreeSet => JTreeSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

import scala.Option.option2Iterable
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.Buffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import JNavigableMapWithAsScalaIterator.javaNavigableMapAsScalaIterator
import org.apache.hadoop.hbase.client.metrics.ScanMetrics

class EmbeddedHTable(
                  val name: String,
                  desc: HTableDescriptor,
                  val conf: Configuration = HBaseConfiguration.create(),
                  private var autoFlush: Boolean = false,
                  private var writeBufferSize: Long = 1,
                  var enabled: Boolean = true,
                  autoFillDesc: Boolean = true,
                  hconnection: EmbeddedHConnection = new EmbeddedHConnection(null)
                ) extends Table with FakeTypes {

  private val Log = LoggerFactory.getLogger(getClass)
  require(conf != null)
  private var closed: Boolean = false
  private val mFakeHConnection: EmbeddedHConnection = hconnection
  private val mHConnection: Connection = UntypedProxy.create(classOf[Connection], mFakeHConnection)

  private var regions: Seq[HRegionLocation] = Seq()

  private final val BytesComparator: java.util.Comparator[Bytes] = Bytes.BYTES_COMPARATOR

  private val rows: Table = new JTreeMap[Bytes, RowFamilies](BytesComparator)

  // -----------------------------------------------------------------------------------------------

  private val mDesc: HTableDescriptor = {
    desc match {
      case null => {
        require(autoFillDesc)
        new HTableDescriptor(TableName.valueOf(name))
      }
      case desc => desc
    }
  }

  override def getName(): TableName = {
    return TableName.valueOf(name)
  }

  override def getConfiguration(): Configuration = {
    return conf
  }

  override def getTableDescriptor(): HTableDescriptor = {
    return mDesc
  }

  override def exists(get: Get): Boolean = {
    return !this.get(get).isEmpty()
  }

  override def append(append: Append): Result = {
    /** Key values to return as a result. */
    val resultKVs = Buffer[KeyValue]()

    synchronized {
      val row = rows.get(append.getRow)

      def getCurrentValue(ce: Cell): Bytes = {
        if (row == null) return null
        val family = row.get(CellUtil.cloneFamily(ce))
        if (family == null) return null
        val qualifier = family.get(CellUtil.cloneQualifier(ce))
        if (qualifier == null) return null
        val entry = qualifier.firstEntry
        if (entry == null) return null
        return entry.getValue
      }

      /** Build a put request with the appended cells. */
      val put = new Put(append.getRow)

      val timestamp = System.currentTimeMillis

      for ((family, ces) <- append.getFamilyCellMap.asScala) {
        for (ce <- ces.asScala) {
          val currentValue: Bytes = getCurrentValue(ce)
          val newValue: Bytes = {
            if (currentValue == null) CellUtil.cloneValue(ce) else (currentValue ++ CellUtil.cloneValue(ce))
          }
          val appendedKV =
            new KeyValue(CellUtil.cloneRow(ce), CellUtil.cloneFamily(ce), CellUtil.cloneQualifier(ce), timestamp, newValue)
          put.add(appendedKV)

          if (append.isReturnResults) resultKVs += appendedKV.clone()
        }
      }
      this.put(put)
    }
    val u = resultKVs.toArray
    return new Result()
  }

  override def get(get: Get): Result = {
    synchronized {
      val filter: Filter = getFilter(get.getFilter)
      filter.reset()
      if (filter.filterAllRemaining()) {
        return new Result()
      }
      val rowKey = get.getRow
      if (filter.filterRowKey(rowKey, 0, rowKey.size)) {
        return new Result()
      }
      val row = rows.get(rowKey)
      if (row == null) {
        return new Result()
      }
      val result = ProcessRow.makeResult(
        table = this,
        rowKey = rowKey,
        row = row,
        familyMap = getFamilyMapRequest(get.getFamilyMap),
        timeRange = get.getTimeRange,
        maxVersions = get.getMaxVersions,
        filter = filter
      )
      if (filter.filterRow()) {
        return new Result()
      }
      return result
    }
  }

  override def get(gets: JList[Get]): Array[Result] = {
    return gets.asScala.map(this.get(_)).toArray
  }

  override def getScanner(scan: Scan): ResultScanner = {
    return new FakeResultScanner(scan)
  }

  override def getScanner(family: Bytes): ResultScanner = {
    return getScanner(new Scan().addFamily(family))
  }

  override def getScanner(family: Bytes, qualifier: Bytes): ResultScanner = {
    return getScanner(new Scan().addColumn(family, qualifier))
  }

  override def put(put: Put): Unit = {
    synchronized {
      val nowMS = System.currentTimeMillis

      val rowKey = put.getRow
      val rowFamilyMap = rows.asScala.getOrElseUpdate(rowKey, new JTreeMap[Bytes, FamilyQualifiers](BytesComparator))
      for ((family, ces) <- put.getFamilyCellMap.asScala) {
        /** Map: qualifier -> time series. */
        val rowQualifierMap = rowFamilyMap.asScala.getOrElseUpdate(family, new JTreeMap[Bytes, ColumnSeries](BytesComparator))

        for (ce <- ces.asScala) {require(Arrays.equals(family, CellUtil.cloneFamily(ce)))

          /** Map: timestamp -> value. */
          val column = rowQualifierMap.asScala.getOrElseUpdate(CellUtil.cloneQualifier(ce), new JTreeMap[JLong, Bytes](TimestampComparator))

          val timestamp = {
            if (ce.getTimestamp == HConstants.LATEST_TIMESTAMP) {
              nowMS
            } else {
              ce.getTimestamp
            }
          }

          column.put(timestamp, CellUtil.cloneValue(ce))
        }
      }
    }
  }

  override def put(put: JList[Put]): Unit = {
    put.asScala.foreach(this.put(_))
  }

  private def checkCell(row: Bytes, family: Bytes, qualifier: Bytes, value: Bytes): Boolean = {
    if (value == null) {
      val fmap = rows.get(row)
      if (fmap == null) return true
      val qmap = fmap.get(family)
      if (qmap == null) return true
      val tmap = qmap.get(qualifier)
      return (tmap == null) || tmap.isEmpty
    } else {
      val fmap = rows.get(row)
      if (fmap == null) return false
      val qmap = fmap.get(family)
      if (qmap == null) return false
      val tmap = qmap.get(qualifier)
      if ((tmap == null) || tmap.isEmpty) return false
      return Arrays.equals(tmap.firstEntry.getValue, value)
    }
  }

  override def checkAndPut(
                            row: Bytes,
                            family: Bytes,
                            qualifier: Bytes,
                            value: Bytes,
                            put: Put
                          ): Boolean = {
    synchronized {
      if (checkCell(row = row, family = family, qualifier = qualifier, value = value)) {
        this.put(put)
        return true
      } else {
        return false
      }
    }
  }

  private def cleanupRow(rowKey: Bytes, family: Option[Bytes], qualifier: Option[Bytes]): Unit = {
    val row = rows.get(rowKey)
    if (row == null) { return }

    val families : Iterable[Bytes] = family match {
      case Some(_) => family
      case None => row.keySet.asScala
    }
    val emptyFamilies = Buffer[Bytes]()  // empty families to clean up
    for (family <- families) {
      val rowQualifierMap = row.get(family)
      if (rowQualifierMap != null) {
        val qualifiers : Iterable[Bytes] = qualifier match {
          case Some(_) => qualifier
          case None => rowQualifierMap.keySet.asScala
        }
        val emptyQualifiers = Buffer[Bytes]()
        for (qualifier <- qualifiers) {
          val timeSeries = rowQualifierMap.get(qualifier)
          if ((timeSeries != null)  && timeSeries.isEmpty) {
            emptyQualifiers += qualifier
          }
        }
        emptyQualifiers.foreach { qualifier => rowQualifierMap.remove(qualifier) }

        if (rowQualifierMap.isEmpty) {
          emptyFamilies += family
        }
      }
    }
    emptyFamilies.foreach { family => row.remove(family) }
    if (row.isEmpty) {
      rows.remove(rowKey)
    }
  }

  override def delete(delete: Delete): Unit = {
    synchronized {
      val rowKey = delete.getRow
      val row = rows.get(rowKey)
      if (row == null) { return }

      if (delete.getFamilyCellMap.isEmpty) {
        for ((family, qualifiers) <- row.asScala) {
          for ((qualifier, series) <- qualifiers.asScala) {
            series.subMap(delete.getTimeStamp, true, 0.toLong, true).clear()
          }
        }
        cleanupRow(rowKey = rowKey, family = None, qualifier = None)
        return
      }

      for ((requestedFamily, ces) <- delete.getFamilyCellMap.asScala) {
        val rowQualifierMap = row.get(requestedFamily)
        if (rowQualifierMap != null) {
          for (ce <- ces.asScala) {
            require(CellUtil.isDelete(ce))
            if (CellUtil.isDeleteFamily(ce)) {
              // Removes versions of an entire family prior to the specified timestamp:
              for ((qualifier, series) <- rowQualifierMap.asScala) {
                series.subMap(ce.getTimestamp, true, 0.toLong, true).clear()
              }
            } else if (CellUtil.isDeleteColumnOrFamily(ce)) {
              // Removes versions of a column prior to the specified timestamp:
              val series = rowQualifierMap.get(CellUtil.cloneQualifier(ce))
              if (series != null) {
                series.subMap(ce.getTimestamp, true, 0.toLong, true).clear()
              }
            } else {
              // Removes exactly one cell:
              val series = rowQualifierMap.get(CellUtil.cloneQualifier(ce))
              if (series != null) {
                val timestamp = {
                  if (ce.getTimestamp == HConstants.LATEST_TIMESTAMP) {
                    series.firstKey
                  } else {
                    ce.getTimestamp
                  }
                }
                series.remove(timestamp)
              }
            }
          }
        }
        cleanupRow(rowKey = rowKey, family = Some(requestedFamily), qualifier = None)
      }
    }
  }

  override def delete(deletes: JList[Delete]): Unit = {
    deletes.asScala.foreach(this.delete(_))
  }

  override def checkAndDelete(
                               row: Bytes,
                               family: Bytes,
                               qualifier: Bytes,
                               value: Bytes,
                               delete: Delete
                             ): Boolean = {
    synchronized {
      if (checkCell(row = row, family = family, qualifier = qualifier, value = value)) {
        this.delete(delete)
        return true
      } else {
        return false
      }
    }
  }

  override def increment(increment: Increment): Result = {
    synchronized {
      val nowMS = System.currentTimeMillis

      val rowKey = increment.getRow
      val row = rows.asScala
        .getOrElseUpdate(rowKey, new JTreeMap[Bytes, FamilyQualifiers](BytesComparator))
      val familyMap = new JTreeMap[Bytes, NavigableSet[Bytes]](BytesComparator)

      for ((family: Array[Byte], qualifierMap: JList[Cell]) <- increment.getFamilyCellMap.asScala) {
        val qualifierSet = familyMap.asScala
          .getOrElseUpdate(family, new JTreeSet[Bytes](BytesComparator))
        val rowQualifierMap = row.asScala
          .getOrElseUpdate(family, new JTreeMap[Bytes, ColumnSeries](BytesComparator))

        for (cell: Cell <- qualifierMap.asScala) {
          val qualifier = CellUtil.cloneQualifier(cell)
          val amount = Bytes.toLong(CellUtil.cloneValue(cell))
          qualifierSet.add(qualifier)
          val rowTimeSeries = rowQualifierMap.asScala.getOrElseUpdate(qualifier, new JTreeMap[JLong, Bytes](TimestampComparator))
          val currentCounter = {
            if (rowTimeSeries.isEmpty) {
              0
            } else {
              Bytes.toLong(rowTimeSeries.firstEntry.getValue)
            }
          }
          val newCounter = currentCounter + amount
          Log.debug("Updating counter from %d to %d".format(currentCounter, newCounter))
          rowTimeSeries.put(nowMS, Bytes.toBytes(newCounter))
        }
      }

      return ProcessRow.makeResult(
        table = this,
        rowKey = increment.getRow,
        row = row,
        familyMap = familyMap,
        timeRange = increment.getTimeRange,
        maxVersions = 1
      )
    }
  }

  override def incrementColumnValue(
                                     row: Bytes,
                                     family: Bytes,
                                     qualifier: Bytes,
                                     amount: Long
                                   ): Long = {
    return this.incrementColumnValue(row, family, qualifier, amount, writeToWAL = true)
  }

  def incrementColumnValue(
                            row: Bytes,
                            family: Bytes,
                            qualifier: Bytes,
                            amount: Long,
                            writeToWAL: Boolean
                          ): Long = {
    val inc = new Increment(row)
      .addColumn(family, qualifier, amount)
    val result = this.increment(inc)
    require(!result.isEmpty)
    return Bytes.toLong(result.getValue(family, qualifier))
  }

  override def mutateRow(mutations: RowMutations): Unit = {
    synchronized {
      for (mutation <- mutations.getMutations.asScala) {
        mutation match {
          case put: Put => this.put(put)
          case delete: Delete => this.delete(delete)
          case _ => sys.error("Unexpected row mutation: " + mutation)
        }
      }
    }
  }

  override def close(): Unit = {
    synchronized {
      this.closed = true
    }
  }

  private[embeddedhbase] def getRegions(): JList[HRegionInfo] = {
    val list = new java.util.ArrayList[HRegionInfo]()
    synchronized {
      for (region <- regions) {
        list.add(region.getRegionInfo)
      }
    }
    return list
  }

  private def toRegions(split: Seq[Bytes]): Iterator[(Bytes, Bytes)] = {
    if (!split.isEmpty) {
      require((split.head != null) && !split.head.isEmpty)
      require((split.last != null) && !split.last.isEmpty)
    }
    val startKeys = Iterator(null) ++ split.iterator
    val endKeys = split.iterator ++ Iterator(null)
    return startKeys.zip(endKeys).toIterator
  }

  private[embeddedhbase] def setSplit(split: Array[Bytes]): Unit = {
    val fakePort = 1234
    val tableName: Bytes = Bytes.toBytes(name)

    val newRegions = Buffer[HRegionLocation]()
    for ((start, end) <- toRegions(split)) {
      val fakeHost = "fake-location-%d".format(newRegions.size)
      val regionInfo = new HRegionInfo(TableName.valueOf(tableName), start, end)
      val seqNum = System.currentTimeMillis()
      newRegions += new HRegionLocation(
        regionInfo,
        ServerName.valueOf(fakeHost, fakePort, /* startCode = */ 0),
        /* seqNum = */ 0
      )
    }
    synchronized {
      this.regions = newRegions.toSeq
    }
  }

  def getRegionLocation(row: String): HRegionLocation = {
    return getRegionLocation(Bytes.toBytes(row))
  }

  def getRegionLocation(row: Bytes): HRegionLocation = {
    return getRegionLocation(row, false)
  }

  def getRegionLocation(row: Bytes, reload: Boolean): HRegionLocation = {
    synchronized {
      for (region <- regions) {
        val start = region.getRegionInfo.getStartKey
        val end = region.getRegionInfo.getEndKey
        // start ≤ row < end:
        if ((Bytes.compareTo(start, row) <= 0)
          && (end.isEmpty || (Bytes.compareTo(row, end) < 0))) {
          return region
        }
      }
    }
    sys.error("Invalid region split: last region must does not end with empty row key")
  }

  def getRegionsInRange(startKey: Bytes, endKey: Bytes): JList[HRegionLocation] = {
    val endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW)
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException("Invalid range: %s > %s".format(
        Bytes.toStringBinary(startKey), Bytes.toStringBinary(endKey)))
    }
    val regionList = new JArrayList[HRegionLocation]()
    var currentKey = startKey
    do {
      val regionLocation = getRegionLocation(currentKey, false)
      regionList.add(regionLocation)
      currentKey = regionLocation.getRegionInfo().getEndKey()
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
      && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0))
    return regionList
  }

  def getConnection(): Connection = {
    mHConnection
  }

  def toHex(bytes: Bytes): String = {
    return bytes.iterator
      .map { byte => "%02x".format(byte) }
      .mkString(":")
  }

  def toString(bytes: Bytes): String = {
    return Bytes.toStringBinary(bytes)
  }

  def dump(out: PrintStream = Console.out): Unit = {
    synchronized {
      for ((rowKey, familyMap) <- rows.asScalaIterator) {
        for ((family, qualifierMap) <- familyMap.asScalaIterator) {
          for ((qualifier, timeSeries) <- qualifierMap.asScalaIterator) {
            for ((timestamp, value) <- timeSeries.asScalaIterator) {
              out.println("row=%s family=%s qualifier=%s timestamp=%d value=%s".format(
                toString(rowKey),
                toString(family),
                toString(qualifier),
                timestamp,
                toHex(value)))
            }
          }
        }
      }
    }
  }

  private def getFilter(filterSpec: Filter): Filter = {
    Option(filterSpec) match {
      case Some(hfilter) => ProtobufUtil.toFilter(ProtobufUtil.toFilter(hfilter))
      case None => PassThroughFilter
    }
  }

  private def getFamilyMapRequest(
                                   request: JMap[Bytes, NavigableSet[Bytes]]
                                 ): NavigableMap[Bytes, NavigableSet[Bytes]] = {
    if (request.isInstanceOf[NavigableMap[_, _]]) {
      return request.asInstanceOf[NavigableMap[Bytes, NavigableSet[Bytes]]]
    }
    val map = new JTreeMap[Bytes, NavigableSet[Bytes]](BytesComparator)
    for ((family, qualifiers) <- request.asScala) {
      map.put(family, qualifiers)
    }
    return map
  }

  private[embeddedhbase] def getFamilyDesc(family: Bytes): HColumnDescriptor = {
    val desc = getTableDescriptor()
    val familyDesc = desc.getFamily(family)
    if (familyDesc != null) {
      return familyDesc
    }
    require(autoFillDesc)
    val newFamilyDesc = new HColumnDescriptor(family)
    // Note on default parameters:
    //  - min versions is 0
    //  - max versions is 3
    //  - TTL is forever
    desc.addFamily(newFamilyDesc)
    return newFamilyDesc
  }

  private class FakeResultScanner(
                                   val scan: Scan
                                 ) extends ResultScanner with JIterator[Result] {

    private val requestedFamilyMap: NavigableMap[Bytes, NavigableSet[Bytes]] =
      getFamilyMapRequest(scan.getFamilyMap)
    private var key: Bytes = {
      synchronized {
        if (rows.isEmpty) {
          null
        } else if (scan.getStartRow.isEmpty) {
          rows.firstKey
        } else {
          rows.ceilingKey(scan.getStartRow)
        }
      }
    }
    if (!scan.getStopRow.isEmpty
      && (key == null || BytesComparator.compare(key, scan.getStopRow) >= 0)) {
      key = null
    }

    val filter = getFilter(scan.getFilter)

    private var nextResult: Result = getNextResult()

    override def hasNext(): Boolean = {
      return (nextResult != null)
    }

    override def next(): Result = {
      val result = nextResult
      nextResult = getNextResult()
      return result
    }

    private def getNextResult(): Result = {
      while (true) {
        getResultForNextRow() match {
          case None => return null
          case Some(result) => {
            if (!result.isEmpty) {
              return result
            }
          }
        }
      }
      // next() returns when a non empty Result is found or when there are no more rows:
      sys.error("dead code")
    }

    private def nextRowKey(): Bytes = {
      if (key == null) { return null }
      val rowKey = key
      key = rows.higherKey(rowKey)
      if ((key != null)
        && !scan.getStopRow.isEmpty
        && (BytesComparator.compare(key, scan.getStopRow) >= 0)) {
        key = null
      }
      return rowKey
    }

    private def getResultForNextRow(): Option[Result] = {
      EmbeddedHTable.this.synchronized {
        filter.reset()
        if (filter.filterAllRemaining) { return None }

        val rowKey = nextRowKey()
        if (rowKey == null) { return None }
        if (filter.filterRowKey(rowKey, 0, rowKey.size)) {
          // Row is filtered out based on its key, return an empty Result:
          return Some(new Result())
        }

        val row = rows.get(rowKey)
        require(row != null)

        val result = ProcessRow.makeResult(
          table = EmbeddedHTable.this,
          rowKey = rowKey,
          row = row,
          familyMap = requestedFamilyMap,
          timeRange = scan.getTimeRange,
          maxVersions = scan.getMaxVersions,
          filter = filter
        )
        if (filter.filterRow()) {
          return Some(new Result())
        }
        return Some(result)
      }
    }

    override def next(nrows: Int): Array[Result] = {
      val results = Buffer[Result]()
      breakable {
        for (nrow <- 0 until nrows) {
          next() match {
            case null => break
            case row => results += row
          }
        }
      }
      return results.toArray
    }

    override def close(): Unit = {
      // Nothing to close
    }

    override def iterator(): JIterator[Result] = {
      return this
    }

    override def remove(): Unit = {
      throw new UnsupportedOperationException
    }

    override def renewLease(): Boolean = ???

    override def getScanMetrics: ScanMetrics = ???
  }
  override def batchCallback[R](
                                 x$1: java.util.List[_ <: Row],
                                 x$2: Array[Object],
                                 x$3: Batch.Callback[R]
                               ): Unit = { sys.error("Not implemented") }

  override def coprocessorService(x$1: Array[Byte]): CoprocessorRpcChannel = {
    sys.error("Not implemented")
  }

  override def incrementColumnValue(
                                     x$1: Array[Byte],
                                     x$2: Array[Byte],
                                     x$3: Array[Byte],
                                     x$4: Long,
                                     x$5: Durability
                                   ): Long = {
    sys.error("Not implemented")
  }

  override def getDescriptor: TableDescriptor = ???

  override def getRegionLocator: RegionLocator = ???
}