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
package org.yardstick.spark.util

import java.lang.{Double => JDouble}
import javafx.application.{Application, Platform}
import javafx.beans.property.{SimpleStringProperty, StringProperty}
import javafx.collections.FXCollections
import javafx.geometry.Insets
import javafx.scene.Scene
import javafx.scene.chart.XYChart._
import javafx.scene.chart.{LineChart, NumberAxis, XYChart}
import javafx.scene.layout.{BorderPane, GridPane}
import javafx.stage.Stage

import scala.collection.SortedMap

/**
 * ReportDataPrep
 *
 */
object ReportDataPrep {

  //  val Title = "IgniteRDD Runtime Performance Compared with Native Spark RDD|Three Workers AWS c3.2xlarge Cluster"
  val Title = "IgniteRDD Runtime Performance Compared with Native Spark RDD|Three Workers AWS c3.2xlarge Cluster"
  val useLogAxis = true

  import collection.mutable

  case class SeriesEntry(groupName: String, inputLine: LLine, native: String, nskew: Int, action: String, nrecs: Int, duration: Int) {
    def seriesName() = s"$nskew $action $native"

    def name() = s"$groupName ${seriesName()}"
  }

  type SeriesValSeq = Seq[SeriesEntry]

  //  case class SeriesMap(smap: Map[String, SeriesValSeq])

  type SeriesInst = mutable.HashMap[String, SeriesValSeq]

  def pr(msg: String) = println(msg)

  case class LLine(tstamp: String, tname: String, nrecs: Int, nparts: Int, nskew: Int,
    native: String, xform: String, action: String, duration: Int, count: Int) {
    def key = s"$tstamp $tname $nparts $nskew $xform $action $native"

    def seriesKey = s"$xform-$action-$native"

    def csvHeader =
      s"TestName,Tstamp,Partitions,SkewFactor,Transform,Action,Native,InputRecords,OutputRecords,Duration"

    def toCsv =
      s"$tname,$tstamp,$nparts,$nskew,$xform,$action,$native,$nrecs,$count,$duration"
  }

  object LLine {
    def apply(line: String) = {
      // testing
      //      val line = "<09:18:00><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native AggregateByKey/Count - duration=14895 millis count=993127"
      //      pr(line)
      val regex = """.*Completed (?<tstamp>[\d]{4}-[\d]{6})/(?<tname>[\w]+) (?<nrecs>[\d]+)[\w]+ (?<nparts>[\d]+)[\w]+ (?<nskew>[\d]+)[\w]+ (?<native>[\w]+) (?<xform>[\w]+)[ /](?<action>[\w]+) - duration=(?<duration>[\d]+) millis count=(?<count>[\d]+).*""".r
      val regex(tstamp, tname, nrecs, nparts, nskew, native, xform, action, duration, count) = line
      new LLine(tstamp, tname, nrecs.toInt / 1000, nparts.toInt, nskew.toInt, native, xform, action, duration.toInt, count.toInt)
    }


  }

  def grabData(baseDir: String) = {
    //    def getFiles(baseDir: String, filter: (File) => java.lang.Boolean): Seq[File] = {
    //      val ffilter = new FileFilter {
    //        override def accept(pathname: File): java.lang.Boolean = filter(pathname)
    //      }
    //      val out: Seq[Seq[File]] = for (f <- new File(baseDir).listFiles(ffilter)) yield {
    //        pr(s"${f.getAbsolutePath}")
    //        f match {
    //          case _ if f.isDirectory => getFiles(f.getAbsolutePath, filter)
    //          case _ if f.isFile => Seq(f)
    //          case _ => throw new IllegalArgumentException(s"Unrecognized file type ${f.getClass.getName}")
    //        }
    //      }
    //      out.flatten
    //    }
    //    val basef = new File(baseDir)
    //    val files = getFiles(baseDir, (path) =>
    //      path.isDirectory || path.getAbsolutePath.endsWith(".log"))
    //    val lines = files.map { f =>
    //      val info = scala.io.Source.fromFile(f.getAbsolutePath)
    //        .getLines.filter(l => l.contains("Completed")).toList.head
    //      LLine(info)
    //    }
    //    pr(s"Number of lines: ${lines.length}")
    //    lines
    //  }
    val data =
      """
        |./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/AggregateByKey/Count.log:<09:18:00><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native AggregateByKey/Count - duration=14895 millis count=993127
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/AggregateByKey/CountByKey.log:<09:18:01><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native AggregateByKey/CountByKey - duration=1121 millis count=993127
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/BasicMap/Count.log:<09:14:46><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native BasicMap Count - duration=43782 millis count=10000000
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/BasicMap/CountByKey.log:<09:15:36><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native BasicMap CountByKey - duration=50388 millis count=993293
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/Filter/Count.log:<09:15:45><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native Filter Count - duration=8540 millis count=6664584
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/Filter/CountByKey.log:<09:16:00><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native Filter CountByKey - duration=15691 millis count=662191
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/GroupByKey/Count.log:<09:17:43><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native GroupByKey/Count - duration=26279 millis count=993277
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/GroupByKey/CountByKey.log:<09:17:45><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native GroupByKey/CountByKey - duration=2786 millis count=993277
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/MapPartitions/Count.log:<09:16:33><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native MapPartitions Count - duration=32780 millis count=10000000
./0730-091340/CoreSmoke/10000000recs/100parts/1skew/native/MapPartitions/CountByKey.log:<09:17:16><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native MapPartitions CountByKey - duration=43219 millis count=993288
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/AggregateByKey/Count.log:<09:19:00><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite AggregateByKey/Count - duration=2165 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/AggregateByKey/CountByKey.log:<09:19:02><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite AggregateByKey/CountByKey - duration=1507 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/BasicMap/Count.log:<09:18:46><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite BasicMap Count - duration=684 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/BasicMap/CountByKey.log:<09:18:49><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite BasicMap CountByKey - duration=2752 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/Filter/Count.log:<09:18:50><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite Filter Count - duration=626 millis count=666666
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/Filter/CountByKey.log:<09:18:52><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite Filter CountByKey - duration=1801 millis count=666666
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/GroupByKey/Count.log:<09:18:57><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite GroupByKey/Count - duration=1845 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/GroupByKey/CountByKey.log:<09:18:58><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite GroupByKey/CountByKey - duration=1481 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/MapPartitions/Count.log:<09:18:52><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite MapPartitions Count - duration=620 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/ignite/MapPartitions/CountByKey.log:<09:18:55><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew ignite MapPartitions CountByKey - duration=2561 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/AggregateByKey/Count.log:<09:14:01><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native AggregateByKey/Count - duration=909 millis count=399375
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/AggregateByKey/CountByKey.log:<09:14:02><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native AggregateByKey/CountByKey - duration=484 millis count=399375
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/BasicMap/Count.log:<09:13:56><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native BasicMap Count - duration=319 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/BasicMap/CountByKey.log:<09:13:57><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native BasicMap CountByKey - duration=897 millis count=399591
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/Filter/Count.log:<09:13:57><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native Filter Count - duration=132 millis count=666494
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/Filter/CountByKey.log:<09:13:57><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native Filter CountByKey - duration=645 millis count=266650
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/GroupByKey/Count.log:<09:14:00><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native GroupByKey/Count - duration=1281 millis count=399606
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/GroupByKey/CountByKey.log:<09:14:01><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native GroupByKey/CountByKey - duration=621 millis count=399606
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/MapPartitions/Count.log:<09:13:58><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native MapPartitions Count - duration=238 millis count=1000000
./0730-091340/CoreSmoke/1000000recs/100parts/1skew/native/MapPartitions/CountByKey.log:<09:13:59><yardstick> Completed 0730-091340/CoreSmoke 1000000recs 100parts 1skew native MapPartitions CountByKey - duration=885 millis count=398997
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/AggregateByKey/Count.log:<09:18:41><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite AggregateByKey/Count - duration=2237 millis count=1000000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/AggregateByKey/CountByKey.log:<09:18:43><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite AggregateByKey/CountByKey - duration=1411 millis count=1000000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/BasicMap/Count.log:<09:18:28><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite BasicMap Count - duration=727 millis count=1000000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/BasicMap/CountByKey.log:<09:18:30><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite BasicMap CountByKey - duration=2426 millis count=1000000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/Filter/Count.log:<09:18:31><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite Filter Count - duration=649 millis count=666666
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/Filter/CountByKey.log:<09:18:33><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite Filter CountByKey - duration=1801 millis count=666666
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/GroupByKey/Count.log:<09:18:38><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite GroupByKey/Count - duration=1899 millis count=1000000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/GroupByKey/CountByKey.log:<09:18:39><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite GroupByKey/CountByKey - duration=1442 millis count=1000000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/MapPartitions/Count.log:<09:18:33><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite MapPartitions Count - duration=657 millis count=1000000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/ignite/MapPartitions/CountByKey.log:<09:18:36><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew ignite MapPartitions CountByKey - duration=2595 millis count=1000000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/AggregateByKey/Count.log:<09:13:55><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native AggregateByKey/Count - duration=425 millis count=58250
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/AggregateByKey/CountByKey.log:<09:13:55><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native AggregateByKey/CountByKey - duration=252 millis count=58250
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/BasicMap/Count.log:<09:13:52><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native BasicMap Count - duration=122 millis count=100000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/BasicMap/CountByKey.log:<09:13:53><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native BasicMap CountByKey - duration=516 millis count=58210
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/Filter/Count.log:<09:13:53><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native Filter Count - duration=80 millis count=66744
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/Filter/CountByKey.log:<09:13:53><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native Filter CountByKey - duration=374 millis count=38882
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/GroupByKey/Count.log:<09:13:54><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native GroupByKey/Count - duration=584 millis count=58209
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/GroupByKey/CountByKey.log:<09:13:55><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native GroupByKey/CountByKey - duration=256 millis count=58209
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/MapPartitions/Count.log:<09:13:54><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native MapPartitions Count - duration=102 millis count=100000
./0730-091340/CoreSmoke/100000recs/100parts/1skew/native/MapPartitions/CountByKey.log:<09:13:54><yardstick> Completed 0730-091340/CoreSmoke 100000recs 100parts 1skew native MapPartitions CountByKey - duration=377 millis count=58265
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/AggregateByKey/Count.log:<09:18:25><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite AggregateByKey/Count - duration=2398 millis count=1000000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/AggregateByKey/CountByKey.log:<09:18:27><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite AggregateByKey/CountByKey - duration=1292 millis count=1000000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/BasicMap/Count.log:<09:18:09><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite BasicMap Count - duration=2260 millis count=1000000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/BasicMap/CountByKey.log:<09:18:12><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite BasicMap CountByKey - duration=3440 millis count=1000000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/Filter/Count.log:<09:18:13><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite Filter Count - duration=933 millis count=666666
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/Filter/CountByKey.log:<09:18:16><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite Filter CountByKey - duration=2304 millis count=666666
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/GroupByKey/Count.log:<09:18:21><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite GroupByKey/Count - duration=2053 millis count=1000000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/GroupByKey/CountByKey.log:<09:18:23><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite GroupByKey/CountByKey - duration=1892 millis count=1000000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/MapPartitions/Count.log:<09:18:17><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite MapPartitions Count - duration=854 millis count=1000000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/ignite/MapPartitions/CountByKey.log:<09:18:19><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew ignite MapPartitions CountByKey - duration=2361 millis count=1000000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/AggregateByKey/Count.log:<09:13:52><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native AggregateByKey/Count - duration=350 millis count=8918
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/AggregateByKey/CountByKey.log:<09:13:52><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native AggregateByKey/CountByKey - duration=172 millis count=8918
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/BasicMap/Count.log:<09:13:48><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native BasicMap Count - duration=6792 millis count=10000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/BasicMap/CountByKey.log:<09:13:49><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native BasicMap CountByKey - duration=1490 millis count=8881
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/Filter/Count.log:<09:13:49><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native Filter Count - duration=134 millis count=6607
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/Filter/CountByKey.log:<09:13:50><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native Filter CountByKey - duration=534 millis count=5910
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/GroupByKey/Count.log:<09:13:51><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native GroupByKey/Count - duration=1016 millis count=8909
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/GroupByKey/CountByKey.log:<09:13:52><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native GroupByKey/CountByKey - duration=380 millis count=8909
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/MapPartitions/Count.log:<09:13:50><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native MapPartitions Count - duration=115 millis count=10000
./0730-091340/CoreSmoke/10000recs/100parts/1skew/native/MapPartitions/CountByKey.log:<09:13:50><yardstick> Completed 0730-091340/CoreSmoke 10000recs 100parts 1skew native MapPartitions CountByKey - duration=460 millis count=8925
      """.stripMargin
    val lines = data.split("\n").filter(_.trim.length > 0).map {
      LLine(_)
    }
    pr(s"Number of lines: ${lines.length}")
    lines.toSeq
  }

  import FxDataUtils._

  import scala.collection.JavaConverters._

  object FxDataUtils {
    type Coords = (JDouble, JDouble, JDouble, JDouble)

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
  }

  def formatData(lines: Seq[ReportDataPrep.LLine]): MapSeriesMap = {
    def formatT(ts: String) = s"${ts.slice(0, 2)}/${ts.slice(2, 4)} ${ts.slice(5, 7)}:${ts.slice(7, 9)}:${ts.slice(9, 11)}"
    def key1(l: LLine) = s"${l.tname} ${l.xform} on ${formatT(l.tstamp)} Partitions=${l.nparts} Skew=${l.nskew}"
    def key2(l: LLine) = s"${l.tname} ${l.tstamp} ${l.nparts} ${l.nskew} ${l.xform} ${l.action} ${l.native}"
    val seriesCollsGrouping = lines.groupBy(key1)
    val sortedSeriesCollsGrouping = SortedMap(seriesCollsGrouping.toList: _*)
    val seriesCollsMap = sortedSeriesCollsGrouping.map { case (k, groupseq) =>
      (k, {
        val unsorted = groupseq.groupBy(key2).map { case (k, ls) => (k, ls.sortBy(_.nrecs)) }
        SortedMap(unsorted.toList: _*)
      }
        )
    }
    val seriesMap = seriesCollsMap.map { case (cgroupkey, cmap) =>
      (cgroupkey, genSeries(cgroupkey, cmap, SeriesPerChart))
    }
    SortedMap(seriesMap.toList: _*)
  }

  def writeToCsv(lines: Seq[LLine]) = {
    val csvLines = lines.map(_.toCsv)
    val out = s"${lines(0).csvHeader}\n${csvLines.mkString("\n")}"
    tools.nsc.io.File("/tmp/lines.csv").writeAll(out)
    out
  }

  val SeriesPerChart = 5

  type SeriesTup = (Series[JDouble, JDouble], Coords)
  type SeriesSeq = Seq[SeriesTup]
  type SeriesMap = SortedMap[String, SeriesTup]
  type MapSeriesMap = SortedMap[String, SeriesMap]

  def genSeries(seriesTag: String, seriesMap: SortedMap[String, Seq[LLine]], maxSeries: Int):
  SeriesMap = {
    if (seriesMap.size > maxSeries) {
      throw new IllegalArgumentException(s"Can not fit > $maxSeries series in a single chart")
    }
    val seriesTups = seriesMap.map { case (sname, serval) =>
      (sname, serval.map(l => double2Double(l.nrecs.toDouble))
        .zip(serval.map(l => double2Double(l.duration.toDouble))))
    }
    val seriesData = seriesTups.map { case (sname, sersSeq) =>
      (sname, sersSeq.map { case (x, y) => new XYChart.Data(x, y) })
    }
    val obsdata = seriesData.map { case (sname, data) =>
      (sname, (new Series(sname.substring(sname.substring(0, sname.lastIndexOf(" ")).lastIndexOf(" ") + 1),
        FXCollections.observableList(data.asJava)), minMax(data)))
    }
    SortedMap(obsdata.toList: _*)
  }

  def display(serGroupName: String, serMap: SortedMap[String, SeriesTup]) = {
    //      saveUsingJavaFx(serGroupName, serMap)
    saveUsingJavaFx()
  }

  class MyApp extends Application {

    def getData(): MapSeriesMap = {
      val path = "/shared/results/0730-091340"
      //    val lines = grabData(args(0))
      val lines = grabData(path)

      val overSeriesMap = ReportDataPrep.formatData(lines)
      overSeriesMap
    }

    val seriesMap = getData()
    val seriesMap2 = getData()

    val ScreenWidth = 1920
    val ScreenHeight = 1200
    val SingleScreenWidth = 800
    val SingleScreenHeight = 600
    val DisplayW = ScreenWidth - 120
    val DefaultDisplayH = ScreenHeight - 100
    val MinChartH = 400
    val Gap = 10
    //    tpane.setPrefColumns(3)
    val MaxCols = 3
    val nCharts = seriesMap.size
    val nCols = math.min(MaxCols, nCharts)
    val nRows = math.ceil(nCharts / nCols)
    val DisplayH = if (nRows <= 3) DefaultDisplayH else (MinChartH + 10) * nRows
    val displaySize = (DisplayW /* / nCols */ , DisplayH, nRows)
    val singleDisplaySize = (SingleScreenWidth  , SingleScreenHeight, nRows)

    def axisCss() = {
      val axis = """
      .axis {
        -fx-font-size: 6px;
        -fx-tick-label-fill: #914800;
        -fx-font-family: Tahoma;
        -fx-tick-length: 20;
        -fx-minor-tick-length: 10;
                 """.stripMargin
      axis
    }

    override def start(mainStage: Stage): Unit = {
      mainStage.setTitle(Title)

      def createChart(ix: Int, chartTitle: String, seriesMap: SeriesMap) = {
        val mm = seriesMap.values.foldLeft(
          new Coords(Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue)) {
          case (c, (series, s)) =>
            new Coords(math.min(c._1, s._1), math.max(c._2, s._2), math.min(c._3, s._3),
              math.max(c._4, s._4))
        }
        val (xAxis, yAxis) = (new LogAxis(math.max(1, mm._1), mm._2),
          new LogAxis(math.max(1, mm._3), mm._4))
        //          (new NumberAxis(math.max(1, mm._1), mm._2, mm._4 / 20),
        //            new NumberAxis(math.max(1, mm._3), mm._4, mm._4 / 20))

        xAxis.setTickLabelRotation(20)
        //        xAxis.setStyle("-fx-tick-label-font-size:0.5em;")
        //        xAxis.setStyle("-fx-axis-font-size:0.5em;")
        //        xAxis.getStyleClass.add(""".axis {
        //                -fx-tick-label-font-size: 1.2em;
        //                -fx-tick-label-fill: -fx-text-background-color;
        //            }
        //          """.stripMargin)

        xAxis.setLabel("Number of Records (Thousands)")
        yAxis.setLabel("Elapsed Time (msecs) (Lower is Better)")
        val lineChart = new LineChart[JDouble, JDouble](xAxis, yAxis)
        //        lineChart.setTitle(s"Ignite-on-Spark Performance - $chartTitle")
        lineChart.setTitle(s"$chartTitle")
        //        lineChart.getStyleClass.add(s"""chart-title {
        //                -fx-font-size: 1.0em;
        //            }""".stripMargin);
        //        lineChart.setStyle(".default-color0.chart-series-line { -fx-stroke: #e9967a; }");

        for ((sname, (series, mm)) <- seriesMap) {
          lineChart.getData().add(series.asInstanceOf[Series[JDouble, JDouble]])
          //          series.nodeProperty.get.setStyle("-fx-stroke-width: 2px;")
          //          lineChart.getData().add(series)
        }
        //        lineChart.setStyle("-fx-chart-title-font-size:1.0em;")
        lineChart
      }
      var firstStage: Stage = null
      // TODO: uncomment the hardcoded single scene
      //      val cssProp = new SimpleStringProperty(axisCss())
      //      val updater = new FXCSSUpdater(scene)
      //      updater.bindCss(cssProp)
      //      ipdater.applyCssToParent(((Parent)popover.getSkin().getNode()));

      val tpane = new GridPane
      tpane.setHgap(10)
      tpane.setVgap(10)
      tpane.setPadding(new Insets(0, 0, 0, 10))
      for ((ix, (sname, series)) <- (0 until seriesMap.size).zip(seriesMap)) {
        val lineChart = createChart(ix, sname, series)
        if (ix == 0) {
          tpane.add(lineChart, math.floor(ix / MaxCols).toInt, ix % MaxCols)
          val scene = new Scene(tpane, displaySize._1, displaySize._2)
          scene.getStylesheets.add("/org/yardstick/spark/util/ys-charts.css")
          mainStage.setScene(scene)
          //          tpane.getChildren.add(ix, lineChart)
          mainStage.show()
        }
        javafx.application.Platform.runLater(new Runnable() {
          override def run() = {
            if (ix > 0) {
              tpane.add(lineChart, math.floor(ix / MaxCols).toInt, ix % MaxCols)
            }
          }
        })
      }
      for ((ix, (sname, series)) <- (0 until seriesMap2.size).zip(seriesMap2)) {
        val lineChart = createChart(ix, sname, series)
        javafx.application.Platform.runLater(new Runnable() {
          override def run() = {
            val stage = new Stage
            val borderPane = new BorderPane(lineChart)
            val scene = new Scene(borderPane, singleDisplaySize._1, singleDisplaySize._2)
            stage.setScene(scene)
            scene.getStylesheets.add("/org/yardstick/spark/util/ys-charts.css")
            stage.show();
          }
        })
      }

    }
  }

    //  def saveUsingJavaFx(sname: String, seriesMap: SortedMap[String,SeriesTup]): Unit = {
    def saveUsingJavaFx(): Unit = {

      Platform.setImplicitExit(false);
      pr("Saving with JavaFX")
      //    val blob = seriesMap.writeObject
      Application.launch(classOf[MyApp], null.asInstanceOf[String])
      pr("launched")

    }

    def testFx() {
      class MyApp extends Application {
        override def start(stage: Stage): Unit = {

          stage.setTitle("Line Chart Sample")
          //defining a series
          val series = new XYChart.Series[JDouble, JDouble]()
          series.setName("My portfolio")
          //populating the series with data
          //        series.getData().addAll(new Array(new XYChart.Data[Double,Double](1, 23)))
          val data = Array.tabulate[XYChart.Data[JDouble, JDouble]](10) { x =>
            new XYChart.Data[JDouble, JDouble](x.toDouble, x.toDouble + x * x + x * x * x)
          }

          import collection.JavaConverters._
          series.setData(FXCollections.observableList(data.toList.asJava))

          //defining the axes
          val mm = FxDataUtils.minMax(data)
          val xAxis = new LogAxis(Math.max(1, mm._1), mm._2)
          val yAxis = new LogAxis(Math.max(1, mm._3), mm._4)

          xAxis.setLabel("Number of Month")
          val lineChart = new LineChart(xAxis, yAxis)

          lineChart.setTitle("Stock Monitoring, 2010")
          val scene = new Scene(lineChart, 800, 600)
          lineChart.getData().add(series)

          stage.setScene(scene)
          stage.show()
        }

        val c = new MyApp
        Application.launch(classOf[MyApp], null.asInstanceOf[String])
      }
    }

  def main(args: Array[String]) {
    //    dumpLines(lines)

    saveUsingJavaFx
    //    for ( (serGroupName, serSeq) <- seriesMap) {
    //      display(serGroupName, serSeq)
    //    }
  }

}
