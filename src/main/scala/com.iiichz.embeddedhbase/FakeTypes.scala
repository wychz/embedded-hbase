package com.iiichz.embeddedhbase

import java.lang.{Long => JLong}
import java.util.NavigableMap

trait FakeTypes {
  type Bytes = Array[Byte]

  type ColumnSeries = NavigableMap[JLong, Bytes]

  type FamilyQualifiers = NavigableMap[Bytes, ColumnSeries]

  type RowFamilies = NavigableMap[Bytes, FamilyQualifiers]

  type Table = NavigableMap[Bytes, RowFamilies]
}