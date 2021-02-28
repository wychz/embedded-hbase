package com.iiichz.embeddedhbase

import scala.util.control.ControlThrowable

class Loop {

  private final val breakException = new BreakControl()

  private final val continueException = new ContinueControl()

  def apply(op: => Unit): Unit = {
    try {
      var loop = true
      while (loop) {
        try {
          op
        } catch {
          case cc: ContinueControl => if (cc != continueException) throw cc // else loop over
        }
      }
    } catch {
      case bc: BreakControl => if (bc != breakException) throw bc // else break this loop
    }
  }

  def continue(): Unit = {
    throw continueException
  }

  def break(): Unit = {
    throw breakException
  }
}

object Loop extends Loop

private class ContinueControl extends ControlThrowable

private class BreakControl extends ControlThrowable

