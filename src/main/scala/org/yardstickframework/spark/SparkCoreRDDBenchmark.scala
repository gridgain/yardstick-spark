package org.yardstickframework.spark

/**
 * Created by sany on 26/6/15.
 */

import org.apache.spark.rdd.RDD
import org.yardstickframework._
import org.yardstickframework.util._
import org.yardstickframework.util.{TimerArray, _}

class SparkCoreRDDBenchmark extends SparkAbstractBenchmark("query") {

  var rdd: RDD[_] = _
  var testOpts: Seq[TestOpt] = _
  val timer = new TimerArray

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration): Unit = {

    super.setUp(cfg)

    testOpts = Seq(
      new TestOpt("string", "SmokeTest", 100, 50, 4, 50, 4, 2, 8, "memory"),
      new TestOpt("int", "SmokeTestMorPartitions", 100, 50, 4, 50, 4, 4, 8, "memory")
    )

  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    for (optsIndex <- 0 until testOpts.size) {
      rdd = new LoadFunctions().coreBattery(sc, optsIndex, testOpts(optsIndex))

      testOpts(optsIndex).dataType match {
        case "string" =>
          var runResults = timer("Sensor-Data") {
            new Oprations().callOperationTest(rdd, "map", "take", "string")
          }
          runResults = timer("Sensor-Data") {
            new Oprations().callOperationTest(rdd, "groupBykey", "countByKey", "string")
          }
          runResults = timer("Sensor-Data") {
            new Oprations().callOperationTest(rdd, "sortByKey", "collect", "string")
          }
        case "int" =>
          var runResults = timer("Sensor-Data") {
            new Oprations().callOperationTest(rdd, "map", "take", "int")
          }
          runResults = timer("Sensor-Data") {
            new Oprations().callOperationTest(rdd, "groupBykey", "countByKey", "int")
          }
          runResults = timer("Sensor-Data") {
            new Oprations().callOperationTest(rdd, "sortByKey", "collect", "int")
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
