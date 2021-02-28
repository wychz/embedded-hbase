package com.iiichz.embeddedhbase

import java.util.{NavigableMap => JNavigableMap}

class JNavigableMapIterator[A, B] (
                                    val underlying: JNavigableMap[A, B]
                                  ) extends Iterator[(A, B)] {
  private val it = underlying.entrySet.iterator

  override def hasNext: Boolean = {
    return it.hasNext
  }

  override def next(): (A, B) = {
    val entry = it.next
    return (entry.getKey, entry.getValue)
  }

  override def hasDefiniteSize: Boolean = {
    return true
  }
}

class JNavigableMapWithAsScalaIterator[A, B](
                                              val underlying: JNavigableMap[A, B]
                                            ) {
  def asScalaIterator(): Iterator[(A, B)] = {
    return new JNavigableMapIterator(underlying)
  }
}

object JNavigableMapWithAsScalaIterator {
  implicit def javaNavigableMapAsScalaIterator[A, B](
                                                      jmap: JNavigableMap[A, B]
                                                    ): JNavigableMapWithAsScalaIterator[A, B] = {
    return new JNavigableMapWithAsScalaIterator(underlying = jmap)
  }
}