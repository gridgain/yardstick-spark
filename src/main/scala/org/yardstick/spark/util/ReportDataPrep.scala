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

import java.awt.geom.{Point2D, Rectangle2D}
import java.awt.{Graphics2D, Dimension, Font, GridLayout}

import org.jfree.chart.annotations.XYTextAnnotation
import org.jfree.chart.axis.{AxisLocation, LogAxis, NumberAxis}
import org.jfree.chart.plot._
import org.jfree.chart.renderer.xy.StandardXYItemRenderer
import org.jfree.chart.{ChartPanel, ChartUtilities, JFreeChart}
import org.jfree.data.xy.XYSeriesCollection

import scala.collection.SortedMap

/**
 * ReportDataPrep
 *
 */
object ReportDataPrep {
  // LLine = namedtuple('LLine', 'tstamp tname nrecs nparts nskew native xform action duration count')

  import collection.mutable

  case class SeriesEntry(groupName: String, inputLine: LLine, native: String, nskew: Int, action: String, nrecs: Int, duration: Int) {
    def seriesName() = s"$nskew $action $native"

    def name() = s"$groupName ${seriesName()}"
  }

  type SeriesValSeq = Seq[SeriesEntry]

  case class SeriesMap(smap: Map[String, SeriesValSeq])

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

  val MaxWidth = 1800
  val MaxHeight = 1000

  object LLine {
    def apply(line: String) = {
      // testing
      //      val line = "<09:18:00><yardstick> Completed 0730-091340/CoreSmoke 10000000recs 100parts 1skew native AggregateByKey/Count - duration=14895 millis count=993127"
      //      pr(line)
      val regex = """.*Completed (?<tstamp>[\d]{4}-[\d]{6})/(?<tname>[\w]+) (?<nrecs>[\d]+)[\w]+ (?<nparts>[\d]+)[\w]+ (?<nskew>[\d]+)[\w]+ (?<native>[\w]+) (?<xform>[\w]+)[ /](?<action>[\w]+) - duration=(?<duration>[\d]+) millis count=(?<count>[\d]+).*""".r
      val regex(tstamp, tname, nrecs, nparts, nskew, native, xform, action, duration, count) = line
      new LLine(tstamp, tname, nrecs.toInt, nparts.toInt, nskew.toInt, native, xform, action, duration.toInt, count.toInt)
    }


  }

  def grabData(baseDir: String) = {
    import java.io._
    def getFiles(baseDir: String, filter: (File) => Boolean): Seq[File] = {
      val ffilter = new FileFilter {
        override def accept(pathname: File): Boolean = filter(pathname)
      }
      val out: Seq[Seq[File]] = for (f <- new File(baseDir).listFiles(ffilter)) yield {
        pr(s"${f.getAbsolutePath}")
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
      val info = scala.io.Source.fromFile(f.getAbsolutePath)
        .getLines.filter(l => l.contains("Completed")).toList.head
      LLine(info)
    }
    pr(s"Number of lines: ${lines.length}") // lines.mkString(","))
    lines
  }

  def genSeries(seriesTag: String, seriesMap: SortedMap[String, Seq[LLine]], maxSeries: Int): XYSeriesCollection = {
    if (seriesMap.size > maxSeries) {
      throw new IllegalArgumentException(s"Can not fit > $maxSeries series in a single chart")
    }
    import scalax.chart.api._
    val scoll = new XYSeriesCollection
    for ((sname, serval) <- seriesMap) {
      val cseries = new XYSeries(sname)
      cseries.add(serval.map(_.nrecs.toDouble).toArray, serval.map(_.duration.toDouble).toArray, true)
      scoll.addSeries(cseries)
    }

    scoll
  }

  def display(scolls: SortedMap[String, XYSeriesCollection]) = {
    // M x 3 grid.
    val N = scolls.size
    val MaxCols = 3
    val nCols = math.max(1, math.min(MaxCols, N))
    val nRows = math.ceil(N / MaxCols).toInt

    import javax.swing._
    val f = new JFrame("Elapsed Time")
    f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    val gridPanel = new JPanel(new GridLayout(1, N))
    //    var subplots = new mutable.ArrayBuffer[(XYPlot, Chart)]()
    val subplots = for ((sx, scoll) <- (0 until scolls.size).zip(scolls)) yield {
      val renderer1 = new StandardXYItemRenderer()
      val rangeAxis1 = new NumberAxis("Elapsed Time (ms)")
      val subplot = new XYPlot(scoll._2, null, rangeAxis1, renderer1) {
        override def draw(g2: Graphics2D, area: Rectangle2D, anchor: Point2D, parentState: PlotState, info: PlotRenderingInfo): Unit = {
//          g2.fill3DRect(5,5,200,200,true)
          g2.drawString(s"Hello there $sx", 300*sx,250*sx)
          println("DRAW !!")
          super.draw(g2, area, anchor, parentState, info)
        }

        override def render(g2: Graphics2D, dataArea: Rectangle2D, index: Int, info: PlotRenderingInfo,
crosshairState: CrosshairState): Boolean = {
//          g2.fill3DRect(5,5,200,200,true)
//          println("RENDER!!")
          super.render(g2, dataArea, index, info, crosshairState)
        }
      }
      subplot.setDomainAxisLocation(AxisLocation.BOTTOM_OR_LEFT)

//      val annotation = new XYTextAnnotation(scoll._1, 150.0, 150.0)
      val annotation = new XYTextAnnotation("Hello", 450.0, 300.0)
      annotation.setFont(new Font("SansSerif", Font.PLAIN, 9))
      annotation.setRotationAngle(Math.PI / 4.0)
      subplot.addAnnotation(annotation)
      subplot
    }
    import util.control.Breaks._
    breakable {
      for (colx <- 0 until nCols) {
        val plot = new CombinedDomainXYPlot(new LogAxis("Number of Records"))
        plot.setGap(10.0)
        plot.setOrientation(PlotOrientation.VERTICAL)
        //        val renderer =  new StandardXYItemRenderer
        for (subplotx <- colx*nCols until Math.min((colx+1)*nCols, N)) {
          plot.add(subplots(subplotx))
        }
        val jfChart = new JFreeChart("",
          JFreeChart.DEFAULT_TITLE_FONT, plot, false)
        //        jfChart.getLegend.setPosition(RectangleEdge.BOTTOM)
        //        jfChart.getLegend.setItemFont(new Font(Font.SANS_SERIF, Font.CENTER_BASELINE, 8))

        ChartUtilities.saveChartAsJPEG(new java.io.File(s"/tmp/chart${colx}.jpg"),
          jfChart, MaxWidth / nCols, MaxHeight)

        val cpanel = new ChartPanel(jfChart)
        cpanel.setPreferredSize(new Dimension(MaxWidth / nCols, MaxHeight))
        gridPanel.add(cpanel, colx)
      }
    }

    // Possibly place the Swing stuff here
    //    ChartUtilities.saveChartAsJPEG(new java.io.File("/tmp/chart.jpg"),
    //      jfChart, 1920, 1200)

    //    ChartJPEGExporter(chart).saveAsJPEG("/tmp/chart.jpg")

    import javax.swing.JInternalFrame
    val jif = new JInternalFrame("Chart", true, true, true, true)
    jif.add(gridPanel)
    jif.pack()
    jif.setVisible(true)
    jif.setMinimumSize(new java.awt.Dimension(MaxWidth, MaxHeight))
    jif.setMaximumSize(new java.awt.Dimension(MaxWidth, MaxHeight))

    val dtp = new JDesktopPane()
    dtp.add(jif)
    f.add(dtp)
    f.pack()
    f.setSize(1800, 1100)
    f.setLocationRelativeTo(null)

    f.setVisible(true)
    //    ChartUtilities.saveChartAsJPEG(new java.io.File("/tmp/chart.jpg"),
    //      jfChart, 1920, 1200)
    //    ChartJPEGExporter(chart).saveAsJPEG("/tmp/chart.jpg")    f.setVisible(true)

  }

  def writeToCsv(lines: Seq[LLine]) = {
    val csvLines = lines.map(_.toCsv)
    val out = s"${lines(0).csvHeader}\n${csvLines.mkString("\n")}"
    tools.nsc.io.File("/tmp/lines.csv").writeAll(out)
    out
  }

  val SeriesPerChart = 5

  def main(args: Array[String]) {
    val lines = grabData(args(0))
    //    dumpLines(lines)
    def key1(l: LLine) = s"${l.tname} ${l.tstamp} ${l.nparts} ${l.nskew} ${l.xform}}"
    def key2(l: LLine) = s"${l.tname} ${l.tstamp} ${l.nparts} ${l.nskew} ${l.xform}} ${l.action} ${l.native}"
    def seriesCollsGrouping = lines.groupBy(key1)
    def sortedSeriesCollsGrouping = SortedMap(seriesCollsGrouping.toList: _*)
    def seriesCollsMap = sortedSeriesCollsGrouping.map { case (k, groupseq) =>
      (k, {
        val unsorted = groupseq.groupBy(key2).map { case (k, ls) => (k, ls.sortBy(_.nrecs)) }
          SortedMap(unsorted.toList:_*)
        }
        )
    }
    def seriesMap = seriesCollsMap.map { case (cgroupkey, cmap) =>
      (cgroupkey, genSeries(cgroupkey, cmap, SeriesPerChart))
    }

    display(seriesMap)
  }

}
