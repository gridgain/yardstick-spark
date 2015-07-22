package org.yardstickframework.spark

import org.apache.spark.SparkContext
import org.yardstickframework._
import org.yardstickframework.spark.util.TimerArray

class SparkCoreRDDBenchmark extends SparkAbstractBenchmark("CoreRDDTests") {

  val timer = new TimerArray(cfg)

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration): Unit = {
    println(s"setUp BenchmarkConfiguration=${cfg.toString}")
    super.setUp(cfg)
  }

  def depthTests(): Boolean = {
    val (pass, tresults) = CoreTestMatrix.runMatrix(sc,IcInfo(ic,icCache))
    pass
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    depthTests()
  }
}

object SparkCoreRDDBenchmark {
  def main(args: Array[String]) {
    val b = new SparkCoreRDDBenchmark
    val cfg = new BenchmarkConfiguration()
    cfg.commandLineArguments(args)
    b.setUp(cfg)
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}
