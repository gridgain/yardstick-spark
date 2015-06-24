package com.yardstickframework.util

/**
 * Created by sany on 23/6/15.
 */
import org.slf4j.LoggerFactory

/**
 * Created by sany.
 */
//todo rename and add some docs
//todo EventsMap?
//todo use TimingInfo
//
case class TimerEntry(name: String, start: Long, var end: Long = -1) {
  def elapsed = end - start
}

class TimerArray {
  val logger = LoggerFactory.getLogger(getClass)
  var timersMap: Map[String, TimerEntry] = Map()

  def start(name: String) = timersMap += name -> TimerEntry(name, System.currentTimeMillis)

  def end(name: String) = timersMap(name).end = System.currentTimeMillis()

  val formatter = java.text.NumberFormat.getIntegerInstance


  def apply[R](name: String)(block: => R): R = {
    logger.info(s"Timeit $name")
    start(name)
    val result = block
    end(name)
    logger.info(s"Timeit ended for $name. Took ${formatTime(timersMap(name).elapsed)} millis")
    result
  }

  private def timeInSec(timeInMillis: Long) = timeInMillis / 1000

  private def formatTime(timeInMillis: Long) = formatter.format(timeInMillis)

  def getTimer(name: String): Option[TimerEntry] = timersMap.get(name)

  def elapsed(name: String): Long = {
    timersMap.get(name) match {
      case None => -1
      case Some(timerEntry) => timerEntry.elapsed
    }
  }

  override def toString: String = {
    timersMap.map(entry => entry._1 -> entry._2.elapsed.toString).toString
  }

  def toFormattedString = {
    timersMap.map(timer => timer._1 -> formatTime(timer._2.elapsed)).toString
  }

  def toString(msg: String): String = {
    msg + toString()
  }

}
