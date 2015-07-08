package org.yardstickframework.ignite

/**
 * Created by sany on 6/7/15.
 */

import java.util.UUID

import org.apache.ignite.Ignition
import org.apache.ignite.spark._

import org.apache.ignite.configuration._
import org.apache.spark._
import org.yardstickframework._

abstract class IgniteAbstractBenchmark extends BenchmarkDriverAdapter {
  var sc: SparkContext = _


  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
    super.setUp(cfg)
    val ignition  = Ignition.start("config/example-cache.xml")
    sc = new SparkContext("local[2]","itest")



  }

  @throws(classOf[Exception])
  override def tearDown() {
    sc.stop

  }
}

