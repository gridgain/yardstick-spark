package org.yardstickframework.spark

/**
 * Created by sany on 26/6/15.
 */

import org.apache.spark.rdd.RDD
import org.yardstickframework._
import org.yardstickframework.util._
import org.yardstickframework.util.{TimerArray, _}

class SparkCoreRDDBenchmark extends SparkAbstractBenchmark("query") {

  var testOpts: Seq[TestOpt] = _
  val timer = new TimerArray

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration): Unit = {

    super.setUp(cfg)

    testOpts = Seq(
      new TestOpt("string", "SmokeTest", 100, 50, 4, 50, 4, 2, 8, "memory"),
      new TestOpt("int", "SmokeTestMorePartitions", 100, 50, 4, 50, 4, 4, 8, "memory")
    )

  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    for (optsIndex <- 0 until testOpts.size) {

      testOpts(optsIndex).dataType match {
        case "string" =>
          val rdd = LoadFunctions.genStringData(sc, optsIndex, testOpts(optsIndex))
          var runResults = timer("Sensor-Data") {
            Operations.stringTransformsTests(rdd, "map", "take")
          }
          runResults = timer("Sensor-Data") {
            Operations.stringTransformsTests(rdd, "sortByKey", "collect")
          }
          runResults = timer("Sensor-Data") {
            Operations.stringAggregationTests(rdd, "groupBykey", "countByKey")
          }
        case "int" =>
          val rdd = LoadFunctions.genIntData(sc, optsIndex, testOpts(optsIndex))
          var runResults = timer("Sensor-Data") {
            Operations.intTransformsTests(rdd, "map", "take")
          }
          runResults = timer("Sensor-Data") {
            Operations.intTransformsTests(rdd, "sortByKey", "collect")
          }
          runResults = timer("Sensor-Data") {
            Operations.intAggregationTests(rdd, "groupBykey", "countByKey")
          }
      }
    }
    true
  }
}

object SparkCoreRDDBenchmark {
  def main(args: Array[String]) {
    val b = new SparkCoreRDDBenchmark
    b.setUp(new BenchmarkConfiguration())
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}
