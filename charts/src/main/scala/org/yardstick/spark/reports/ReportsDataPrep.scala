package org.yardstick.spark.reports

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

import java.io.File
import java.lang.{Double => JDouble}
import javafx.scene.chart.XYChart
import javafx.scene.chart.XYChart.Series

import org.yardstick.spark.reports.SqlReportsDataPrep.LLine

import scala.collection.SortedMap

/**
 * ReportsDataPrep
 *
 */
object ReportsDataPrep {

  def pr(msg: String) = println(msg)

  type Coords = (JDouble, JDouble, JDouble, JDouble)
  type SeriesTup = (Series[JDouble, JDouble], Coords)
  type SeriesSeq = Seq[SeriesTup]
  type SeriesMap = SortedMap[String, SeriesTup]
  type MapSeriesMap = SortedMap[String, SeriesMap]

  def grabData(baseDir: String) = {
    def getFiles(baseDir: String, filter: (File) => Boolean): Seq[File] = {
      val out: Seq[Seq[File]] = for (f <- new File(baseDir).listFiles.filter(filter))
        yield {
          //          pr(s"${f.getAbsolutePath}")
          f match {
            case _ if f.isDirectory => getFiles(f.getAbsolutePath, filter)
            case _ if f.isFile => Seq(f)
            case _ => throw new IllegalArgumentException(s"Unrecognized file type ${f.getClass.getName}")
          }
        }
      out.flatten
    }
    val basef = new File(baseDir)
    val files = getFiles(baseDir, (path) =>
      path.isDirectory || path.getAbsolutePath.endsWith(".log"))
    val lines = files.map { f =>
      pr(s"processing ${f.getAbsolutePath}")
      val info = scala.io.Source.fromFile(f.getAbsolutePath)
        .getLines.filter(l => l.contains("Completed")).toList
      if (!info.isEmpty) Some(info.head) else None
    }.flatten
    pr(s"Number of lines: ${lines.length}")
    lines
  }

  def minMax(data: Seq[XYChart.Data[JDouble, JDouble]]): Coords = {
    data.foldLeft[Coords]((Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue)) { case (l, d) => {
      (if (l._1 < d.getXValue.doubleValue)
        l._1
      else
        d.getXValue.doubleValue,
        if (l._2 > d.getXValue.doubleValue)
          l._2
        else
          d.getXValue.doubleValue,
        if (l._3 < d.getYValue.doubleValue) l._3 else d.getYValue.doubleValue,
        if (l._4 > d.getYValue.doubleValue) l._4 else d.getYValue.doubleValue
        )
    }
    }
  }

  def writeToCsv(lines: Seq[LLine], fileName: String) = {
    val csvLines = lines.map(_.toCsv)
    val out = s"${lines(0).csvHeader}\n${csvLines.mkString("\n")}"
    scala.tools.nsc.io.File(fileName).writeAll(out)
    out
  }

}
