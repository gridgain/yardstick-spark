package org.yardstickframework.ignite

/**
 * Created by sany on 6/7/15.
 */

import java.util.UUID

import org.apache.ignite.Ignition
import org.apache.ignite.spark._

import org.apache.ignite.configuration._
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext
import org.yardstickframework._

abstract class IgniteAbstractBenchmark extends BenchmarkDriverAdapter {
  var sc: SparkContext = _
  var sqlContext: HiveContext = _

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
    super.setUp(cfg)
    val ignition  = Ignition.start("config/example-cache.xml")
    sc = new SparkContext("local[2]","itest")
    sqlContext = new HiveContext(sc)


  }

  @throws(classOf[Exception])
  override def tearDown() {
    sc.stop

  }
}

