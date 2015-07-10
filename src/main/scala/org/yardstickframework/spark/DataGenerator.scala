package org.yardstickframework.spark

import java.io.Serializable

import org.apache.ignite.spark.IgniteContext
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.yardstickframework.spark.YsSparkTypes._
import collection.mutable
import scala.util.Random
import org.yardstickframework.spark.util.YardstickLogger._

object YsSparkTypes {
  type RddKey = Long
  type RddVal = String
  type RddTuple = (RddKey, RddVal)
  type InputRDD = RDD[RddTuple]
  type XformRDD = RDD[RddTuple]
  type AggRDD = RDD[(RddKey, Iterable[RddVal])]
  type CountRDD = RDD[(RddKey, Long)]

  /**
  abstract case class ProbDistFn()() {
    def genOne(min: Long, max: Long): Double
  }

  case class EqualRangesPartitioner(min: Long, max: Long, override val numPartitions: Int) extends Partitioner {
    assert(max - min > 0, "Hi! Please try to make your max greater than your min. Ten-four.")
    assert(numPartitions > 0, "Hi! Black-hole style partitions are disallowed at this time.")
    val psize = ((max - min) / numPartitions).toInt
    val partitions = for (p <- 0 until numPartitions) yield {

    }

    override def getPartition(key: Any): Int = {
      assert(key.isInstanceOf[Long], "Let us get some LONG keys please")
      val pnum = key.asInstanceOf[Long] % psize
      assert(pnum < Int.MaxValue, s"Houston, we have an uncooperative key (> MaxInt) here: $pnum")
      pnum.toInt
    }
  }

  case class SkewGen(sc: SparkContext, nPartitions: Int, nRecords: Int, namePrefix: String,
    min: Long, max: Long, probFn: ProbDistFn) {
    def genData() = {
      sc.parallelize(for (rx <- 0 until nRecords) yield {
        val rec = (s"$namePrefix-$rx", probFn.genOne(min, max))
        rec
      }, nPartitions)
    }.partitionBy(EqualRangesPartitioner(min, max, nPartitions))

  }
    * */
  sealed abstract class Action(name: String)

  case object Collect extends Action("collect")

  case object CollectByKey extends Action("collectByKey")

  case object CollectAsMap extends Action("collectAsMap")

}


case class GenDataParams(nRecords: Int, nPartitions: Int, optMin: Option[Long] = None, optMax: Option[Long] = None,
  optSkewFactor: Option[Int] = Some(1)) extends Serializable

abstract class DataGenerator(dataParams: GenDataParams, optRddIn: Option[InputRDD]) extends Serializable {
  var optData = optRddIn

  def getData() = optRddIn.getOrElse(genData(Some(dataParams.optMin.get), Some(dataParams.optMax.get)))

  def getData(min: Long, max: Long) = optRddIn.getOrElse(genData(Some(min), Some(max)))

  def genData(optMin: Option[Long] = None, optMax: Option[Long] = None): InputRDD = optRddIn.get
}


class ProvidedDataGenerator(sc: SparkContext, dataParams: GenDataParams)(rddIn: InputRDD) extends DataGenerator(dataParams, Some(rddIn))

class FileDataGenerator(sc: SparkContext, dataParams: GenDataParams, path: String, delim: Char) extends ProvidedDataGenerator(sc, dataParams)(
{
  val rdd = sc.textFile(path, dataParams.nPartitions)
  rdd.map { l =>
    val toks = l.split(delim)
    (toks(0).toLong, toks(1))
  }
}) {}

class SingleSkewDataGenerator(sc: SparkContext, optIcInfo: Option[IcInfo], dataParams: GenDataParams)
  extends DataGenerator(dataParams, None) {
  override def genData(optMin: Option[Long] = dataParams.optMin, optMax: Option[Long] = dataParams.optMax) = {

    val dataToBc = new java.io.Serializable {
      val params = dataParams
      val nPartitions = dataParams.nPartitions
      val nrecs = Math.ceil(dataParams.nRecords / (dataParams.nPartitions * 2.0)).toInt
      val width = (optMax.get - optMin.get) / dataParams.nPartitions
      val firstNrec = dataParams.nRecords - (dataParams.nPartitions - 1) * nrecs
      val nrecsAndBounds = (0 until nPartitions)
        .foldLeft(new mutable.ArrayBuffer[(Int, (Long, Long))]()) { case (m, rx) =>
        m += Tuple2(if (rx == 0) firstNrec else nrecs, (1L * rx * width, (rx + 1L) * width))
      }
      val words = DataGeneratorUtils.readWords
      println(s"len(words) is ${words.length}")
      val nWords = words.size
    }

    val rdd = if (optIcInfo.isDefined) {
      val localData = sc.parallelize(
      {
        val rnd = new java.util.Random
        var longs = new java.util.Random().longs(dataToBc.nrecs, optMin.get, optMax.get).iterator
        val out = (0 until dataToBc.nrecs).foldLeft(mutable.ArrayBuffer[RddTuple]()) { case (m, n) =>
          val windex = rnd.nextInt(dataToBc.nWords)
          m += Tuple2(longs.next, dataToBc.words(windex))
        }
        out
      }
      , dataParams.nPartitions)
      val drdd = optIcInfo.get.icCache.savePairs(localData)
      optIcInfo.get.icCache
    } else {
      val bcData = sc.broadcast(dataToBc)
      val localData = sc.parallelize((0 until dataParams.nPartitions).toSeq, dataParams.nPartitions)
      val rdd = localData.mapPartitionsWithIndex { case (partx, iter) =>
        val rnd = new java.util.Random
        val iterout = iter.map { ix =>
          val locData = bcData.value
          val (nrecs, (lbound, ubound)) = locData.nrecsAndBounds(partx)
          assert(nrecs > 0, s"nrecs is not positive $nrecs")
          assert(ubound > lbound, s"ubound $ubound < lbound $lbound")
          var longs = new java.util.Random().longs(nrecs, lbound, ubound).iterator
          val out = (0 until nrecs).foldLeft(mutable.ArrayBuffer[RddTuple]()) { case (m, n) =>
            val windex = rnd.nextInt(locData.nWords)

            // println(s"Word index = $windex")
            m += Tuple2(longs.next, locData.words(windex))
          }
          out
        }
        iterout.flatten
      }
      rdd
    }
    //    if (optIcInfo.isDefined) {
    //      optIcInfo.get.icCache.savePairs(rdd)
    //      optIcInfo.get.icCache
    //    } else {
    rdd
    //    }
  }
}


object DataGeneratorUtils {
  def readWords() = {
    val text = scala.io.Source.fromFile("src/main/resources/aliceInWonderland.txt").mkString("")
    println(s"alice textlen = ${text.size}")
    val rmtext = text.map { c => c match {
      case _ if """~$!@#`%^&*()-_=+[{}}'";;,.<>?""".contains(c) => ' '
      case '\n' => ' '
      case _ => c
    }
    }.toString
    val words = rmtext.split(" ")
    words
  }

}