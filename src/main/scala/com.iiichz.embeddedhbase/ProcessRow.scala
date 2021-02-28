package com.iiichz.embeddedhbase

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList}
import java.util.Arrays
import java.util.{List => JList}
import java.util.NavigableMap
import java.util.NavigableSet

import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.io.TimeRange
import JNavigableMapWithAsScalaIterator.javaNavigableMapAsScalaIterator
import org.slf4j.LoggerFactory

object ProcessRow extends FakeTypes {
  private final val Log = LoggerFactory.getLogger("ProcessRow")

  private final val KeyValueComparator = KeyValue.COMPARATOR

  final val EmptyBytes = Array[Byte]()

  private final val FamilyLoop = new Loop()
  private final val QualifierLoop = new Loop()
  private final val TimestampLoop = new Loop()

  private final val EmptyKeyValue =
    new KeyValue(EmptyBytes, EmptyBytes, EmptyBytes, HConstants.LATEST_TIMESTAMP, EmptyBytes)

  def makeResult(
                  table: EmbeddedHTable,
                  rowKey: Bytes,
                  row: RowFamilies,
                  familyMap: NavigableMap[Bytes, NavigableSet[Bytes]],
                  timeRange: TimeRange,
                  maxVersions: Int,
                  filter: Filter = PassThroughFilter
                ): Result = {

    val cells: JList[Cell] = new JArrayList[Cell]

    val nowMS = System.currentTimeMillis

    var start: Cell = EmptyKeyValue

    val families: NavigableSet[Bytes] =
      if (!familyMap.isEmpty) familyMap.navigableKeySet else row.navigableKeySet

    var family: Bytes = families.ceiling(CellUtil.cloneFamily(start))

    FamilyLoop {
      if (family == null) FamilyLoop.break

      val rowQMap = row.get(family)
      if (rowQMap == null) {
        family = families.higher(family)
        FamilyLoop.continue
      }

      // Apply table parameter (TTL, max/min versions):
      {
        val familyDesc: HColumnDescriptor = table.getFamilyDesc(family)

        val maxVersions = familyDesc.getMaxVersions
        val minVersions = familyDesc.getMinVersions
        val minTimestamp = nowMS - (familyDesc.getTimeToLive * 1000L)

        for ((qualifier, timeSeries) <- rowQMap.asScalaIterator) {
          while (timeSeries.size > maxVersions) {
            timeSeries.remove(timeSeries.lastKey)
          }
          if (familyDesc.getTimeToLive != HConstants.FOREVER) {
            while ((timeSeries.size > minVersions)
              && (timeSeries.lastKey < minTimestamp)) {
              timeSeries.remove(timeSeries.lastKey)
            }
          }
        }
      }

      val qualifiers: NavigableSet[Bytes] = {
        val reqQSet = familyMap.get(family)
        (if ((reqQSet == null) || reqQSet.isEmpty) rowQMap.navigableKeySet else reqQSet)
      }

      var qualifier: Bytes = qualifiers.ceiling(CellUtil.cloneQualifier(start))
      QualifierLoop {
        if (qualifier == null) QualifierLoop.break

        val series: ColumnSeries = rowQMap.get(qualifier)
        if (series == null) {
          qualifier = qualifiers.higher(qualifier)
          QualifierLoop.continue
        }

        val versionMap = series.subMap(timeRange.getMax, false, timeRange.getMin, true)

        val timestamps: NavigableSet[JLong] = versionMap.navigableKeySet

        var nversions = 0

        var timestamp: JLong = timestamps.ceiling(start.getTimestamp.asInstanceOf[JLong])
        TimestampLoop {
          if (timestamp == null) TimestampLoop.break

          val value: Bytes = versionMap.get(timestamp)
          val kv: KeyValue = new KeyValue(rowKey, family, qualifier, timestamp, value)

          filter.filterKeyValue(kv) match {
            case Filter.ReturnCode.INCLUDE => {
              cells.add(filter.transformCell(kv))
              nversions += 1
              if (nversions >= maxVersions) {
                TimestampLoop.break
              }
            }
            case Filter.ReturnCode.INCLUDE_AND_NEXT_COL => {
              cells.add(filter.transformCell(kv))
              TimestampLoop.break
            }
            case Filter.ReturnCode.SKIP =>
            case Filter.ReturnCode.NEXT_COL => TimestampLoop.break
            case Filter.ReturnCode.NEXT_ROW => {
              QualifierLoop.break
            }
            case Filter.ReturnCode.SEEK_NEXT_USING_HINT => {
              Option(filter.getNextCellHint(kv)) match {
                case None =>
                case Some(hint) => {
                  require(KeyValueComparator.compare(kv, hint) < 0,
                    "Filter hint cannot go backward from %s to %s".format(kv, hint))
                  if (!Arrays.equals(rowKey, CellUtil.cloneRow(hint))) {
                    FamilyLoop.break
                  }
                  start = hint
                  if (!Arrays.equals(family, CellUtil.cloneFamily(hint))) {
                    family = families.ceiling(CellUtil.cloneFamily(hint))
                    FamilyLoop.continue
                  } else if (!Arrays.equals(qualifier, CellUtil.cloneQualifier(hint))) {
                    qualifier = qualifiers.ceiling(CellUtil.cloneQualifier(hint))
                    QualifierLoop.continue
                  } else {
                    timestamp = timestamps.higher(hint.getTimestamp.asInstanceOf[JLong])
                    TimestampLoop.continue
                  }
                }
              }
            }
          }

          timestamp = timestamps.higher(timestamp).asInstanceOf[JLong]
        }
        qualifier = qualifiers.higher(qualifier)
      }

      family = families.higher(family)
    }

    if (filter.hasFilterRow()) {
      filter.filterRowCells(cells)
    }
    return Result.create(cells)
  }

}
