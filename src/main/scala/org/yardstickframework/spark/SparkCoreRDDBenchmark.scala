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
package org.yardstickframework.spark

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.yardstickframework._
import org.yardstickframework.spark.YsSparkTypes.{RddKey, RddVal}

import SparkCoreRDDBenchmark._

/**
 * How to run this in Intellij :
 *
 *  You need 8GB of free memory to run the 100M test.  Comment that test out in CoreBattery if you do not
 *  have sufficient memory
 *
 *  1. Create a new run configuration for this class - pointing to the main() method
 *
 *  2. Set JVM Options to
 *       -DIGNITE_QUIET=false  -Xloggc:./gc.log  -XX:+PrintGCDetails  -verbose:gc  -XX:+UseParNewGC
 *          -XX:+UseConcMarkSweepGC  -XX:+UseTLAB  -XX:NewSize=128m  -XX:MaxNewSize=128m
 *          -Xms1024m  -Xmx8192m  -XX:MaxPermSize=512m  -XX:MaxTenuringThreshold=0
 *          -XX:SurvivorRatio=1024  -XX:+UseCMSInitiatingOccupancyOnly
 *          -XX:CMSInitiatingOccupancyFraction=60
 *  3. Set Program Arguments to
 *      -cfg /mnt/thirdeye/yardstick-spark/config/ignite-localhost-config.xml -nn 1 -v -b 1 -w 60 -d 10
 *          -t 1  -sm PRIMARY_SYNC -dn SparkCoreRDDBenchmark -cn tx -sn SparkNode
 *
 */
class SparkCoreRDDBenchmark extends SparkAbstractBenchmark[RddKey,RddVal](CORE_CACHE_NAME) {

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration): Unit = {
    println(s"setUp BenchmarkConfiguration=${cfg.toString}")
    super.setUp(cfg)
  }

  def depthTests(): Boolean = {
    val (pass, tresults) = CoreTestMatrix.runMatrix(sc, cacheName)
    pass
  }

  // Following test is taken from IgniteRDDSpec in the original Ignite distribution
  def icTest() = {
    type TestKey = String
    type TestVal = Entity
    val ic = new IgniteContext[TestKey,TestVal](sc,
      () ⇒ new IgniteConfiguration())

    try {
      val cache: IgniteRDD[TestKey,TestVal] = ic.
        fromCache(new TestCacheConfiguration[String,Entity]().cacheConfiguration(cacheName))

      import ic.sqlContext.implicits._

      cache.savePairs(sc.parallelize(0 to 1000, 2).map(i ⇒ (String.valueOf(i), new Entity(i, "name" + i, i * 100))))

      val df = cache.sql("select id, name, salary from Entity where name = ? and salary = ?", "name50", 5000)

      df.printSchema()

      val res = df.collect()

      println(s"Retrieved ${res.size} records. First one is ${res(0).toString}")
      assert(res.length == 1, "Invalid result length")
      assert(50 == res(0)(0), "Invalid result")
      assert("name50" == res(0)(1), "Invalid result")
      assert(5000 == res(0)(2), "Invalid result")

      val df0 = cache.sql("select  id, name, salary from Entity").where('NAME === "name50" and 'SALARY === 5000)

      val res0 = df0.collect()

      assert(res0.length == 1, "Invalid result length")
      assert(50 == res0(0)(0), "Invalid result")
      assert("name50" == res0(0)(1), "Invalid result")
      assert(5000 == res0(0)(2), "Invalid result")

      assert(500 == cache.sql("select id from Entity where id > 500").count(), "Invalid count")
    }
    finally {
      ic.close()
    }
    true
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
//    icTest()
    depthTests()
  }
}

object SparkCoreRDDBenchmark {
  val CORE_CACHE_NAME = "core"
  def main(args: Array[String]) {
    val b = new SparkCoreRDDBenchmark
    val cfg = new BenchmarkConfiguration()
    cfg.commandLineArguments(args)
    b.setUp(cfg)
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}
