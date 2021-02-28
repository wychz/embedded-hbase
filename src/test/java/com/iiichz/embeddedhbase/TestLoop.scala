package com.iiichz.embeddedhbase

import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory

class TestLoop {
  import TestLoop.Log

  @Test
  def testLoop(): Unit = {
    val loop1 = new Loop()
    val loop2 = new Loop()

    var loop1Counter = 0
    var loop2Counter = 0
    var iloop1 = 0
    loop1 {
      if (iloop1 >= 2) loop1.break()
      iloop1 += 1

      loop1Counter += 1

      var iloop2 = 0
      loop2 {
        if (iloop2 >= 3) loop2.break()
        iloop2 += 1

        loop2Counter += 1
      }
    }
    Assert.assertEquals(2, loop1Counter)
    Assert.assertEquals(6, loop2Counter)
  }

  @Test
  def testNestedBreak(): Unit = {
    val loop1 = new Loop()
    val loop2 = new Loop()

    var loop1Counter = 0
    var loop2Counter = 0
    loop1 {
      loop1Counter += 1
      loop2 {
        loop2Counter += 1
        loop1.break()
      }
    }
    Assert.assertEquals(1, loop1Counter)
    Assert.assertEquals(1, loop2Counter)
  }

  @Test
  def testContinue(): Unit = {
    val loop1 = new Loop()
    val loop2 = new Loop()

    var loop1Counter = 0
    var loop2Counter = 0

    var iloop1 = 0
    loop1 {
      if (iloop1 >= 2) loop1.break()
      iloop1 += 1

      loop1Counter += 1

      var iloop2 = 0
      loop2 {
        if (iloop2 >= 1) loop2.break()
        iloop2 += 1

        loop2Counter += 1
        loop1.continue()
        Assert.fail("Should not happen!")
      }

    }
    Assert.assertEquals(2, loop1Counter)
    Assert.assertEquals(2, loop2Counter)
  }
}

object TestLoop {
  private final val Log = LoggerFactory.getLogger(classOf[TestLoop])
}
