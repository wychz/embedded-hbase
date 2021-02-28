package com.iiichz.embeddedhbase

import org.apache.hadoop.hbase.filter.{Filter, FilterBase}

/** Pass-through HBase filter, ie. behaves as if there were no filter. */
object PassThroughFilter extends FilterBase {
  def parseFrom(bytes: Array[Byte]): Filter = {
    this
  }
}