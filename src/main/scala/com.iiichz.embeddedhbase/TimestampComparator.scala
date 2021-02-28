package com.iiichz.embeddedhbase

import java.util.Comparator
import java.lang.{Long => JLong}

object TimestampComparator extends Comparator[JLong] {
  override def compare(long1: JLong, long2: JLong): Int = {
    return long2.compareTo(long1)
  }
}
