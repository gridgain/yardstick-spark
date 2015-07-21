/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.yardstickframework.spark.util

import java.io.{FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.slf4j.LoggerFactory
import org.yardstickframework.BenchmarkConfiguration
import org.yardstickframework.spark.util.YardstickLogger._

class TimerArray(cfg: BenchmarkConfiguration) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger(getClass)
  var timersMap: Map[String, TimerEntry] = Map()

  def start(name: String) = timersMap += name -> TimerEntry(name, System.nanoTime())

  def end(name: String) = timersMap(name).end = System.nanoTime()

  val formatter = java.text.NumberFormat.getIntegerInstance


  def apply[R](name: String)(block: => R): R = {
    trace(name, s"Starting Timed Block $name ")
    start(name)
    val result = block
    end(name)
    trace(name, s"Completed Timed Block $name - duration=${formatTime(timersMap(name).elapsed) /* (1000*1000) */} millis")
    result
  }

  private def timeInSec(timeInNano: Long) = timeInNano / 1000000

  private def formatTime(timeInNano: Long) = formatter.format(timeInNano)

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
