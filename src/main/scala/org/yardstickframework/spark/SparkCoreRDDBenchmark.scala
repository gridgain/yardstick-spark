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
import org.yardstickframework.spark.util.TimerArray

class SparkCoreRDDBenchmark extends SparkAbstractBenchmark[RddKey,RddVal]("core") {

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

  // Following test is taken from IgniteRDDSpec in the original Ignite distribution
  def icTest()   {
    type TestKey = String
    type TestVal = Entity
//    val sc = new SparkContext("local[*]", "test")
    val ic = new IgniteContext[TestKey,TestVal](sc,
      () ⇒ new IgniteConfiguration())

    try {
      val cache: IgniteRDD[TestKey,TestVal] = ic.
        fromCache(new TestCacheConfiguration[String,Entity]().cacheConfiguration("client"))

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
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    icTest()
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
