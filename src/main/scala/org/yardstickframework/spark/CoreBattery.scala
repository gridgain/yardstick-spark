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

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.yardstickframework.spark.YsSparkTypes._
import org.yardstickframework.spark.util.YardstickLogger._

import scala.collection.mutable

case class TestResult(testName: String, resultName: String,
  optCount: Option[Int] = None, optResult: Option[Any] = None, stdOut: Option[String] = None,
  stdErr: Option[String] = None)

abstract class TestBattery(name: String, outdir: String) {
  def setUp(): Unit = {}
  def runBattery(): (Boolean, Seq[TestResult])
  def tearDown(): Unit = {}
}

case class TestParams(name: String, dataParams: GenDataParams)

case class TestMatrixSpec(name: String, version: String, genDataParams: GenDataParams)

object CoreTestMatrix {
  val A = Array
  def runMatrix(sc: SparkContext, icInfo: IcInfo) = {
    val testDims = new {
      var nRecords = A(100, 1000)
      var nPartitions = A(1, 10, 100)
      var firstPartitionSkew = A(1, 10, 100)
      val min = 0L
      val max = 20000L
    }
    val passArr = mutable.ArrayBuffer[Boolean]()
    val resArr = mutable.ArrayBuffer[TestResult]()
    val dtf = new SimpleDateFormat("MMdd-hhmmss").format(new Date)
    for (nRecs <- testDims.nRecords;
         nPartitions <- testDims.nPartitions;
         skew <- testDims.firstPartitionSkew;
         useIgnite <- A(false,true)) {

      val rawname = "CoreSmoke"
      val tname = s"$dtf/$rawname"
      val igniteOrNative = if (useIgnite) "ignite" else "native"
      val name = s"$tname ${nRecs}recs ${nPartitions}parts ${skew}skew ${igniteOrNative}"
      val dir = name.replace(" ","/")
      val mat = TestMatrixSpec("core-smoke", "0.1", GenDataParams(nRecs, nPartitions, Some(testDims.min), Some(testDims.max), Some(skew) ))
      val optIcInfo = if (useIgnite) Some(icInfo) else None
      val dgen = new SingleSkewDataGenerator(sc, optIcInfo, mat.genDataParams)
      val rdd = dgen.genData()
      val battery = new CoreBattery(sc, optIcInfo, name, dir, rdd)
      val (pass, tresults) = battery.runBattery()
      passArr += pass
      resArr ++= tresults
    }
    (passArr.forall(identity), resArr)
  }
}

object CoreBattery {

  def runBattery(): Unit = {

  }
  def main(args: Array[String]) {

  }
}

class CoreBattery(sc: SparkContext, optIcInfo: Option[IcInfo],
  testName: String, outputDir: String, inputRdd: InputRDD) extends TestBattery("CoreBattery", s"$outputDir/$testName") {
  assert(inputRdd != null,"Hey null RDD's are not cool")
  override def runBattery() = {
    val xformRdds = Seq(
      (s"$testName/BasicMap", inputRdd.map { case (k,v) =>
        (k, s"[${k}]:$v")
      }),
      (s"$testName/Filter", inputRdd.filter { case (k, v) =>
        k % 3 != 0
      }),
      (s"$testName/MapPartitions", {
        val sideData = Range(0, 100000)
        val bcSideData = sc.broadcast(sideData)
        val irdd = inputRdd.mapPartitions { iter =>
        iter.map { case (k, v) =>
          val localData = bcSideData.value
          // println(s"localdata.size=${localData.size}")
          (k,1000.0 + v)
        }
      }; irdd }))
    val actions = Seq(Collect, CollectByKey)
    val res = for ((name, rdd) <- xformRdds) yield {
      runXformTests(name, rdd, actions)
    }
    val aggRdds = Seq(
      (s"$testName/GroupByKey", inputRdd.groupByKey)
      )
    val aggActions = Seq(Collect, CollectByKey)
    val ares = for ((name, rdd) <- aggRdds) yield {
      runAggTests(name, rdd, aggActions)
    }
    val countRdds = Seq(
//      (s"$testName-countByKey", inputRdd.countByKey)
      (s"$testName/AggregateByKey", inputRdd.aggregateByKey(0L)((k,v) => k*v.length, (k,v) => k+v))
      )
    val countActions = Seq(Collect, CollectByKey)
    val cres = for ((name, rdd) <- countRdds) yield {
      runCountTests(name, rdd, countActions)
    }
    // TODO: determine a pass/fail instead of returning true
    (true, (res ++ ares ++ cres).flatten)
  }

  def getSize(x: Any) = {
    x match {
        case arr: Array[_]  => arr.length
        case m: mutable.Map[_,_]  => m.size
        case _ => throw new IllegalArgumentException(s"What is our type?? ${x.getClass.getName}")
      }
  }
  def runXformTests(name: String, rdd: InputRDD, actions: Seq[Action]): Seq[TestResult] = {
    val results = for (action <- actions) yield {
      val tname = s"$name/$action"
      trace(tname, s"Starting xform test $tname")
      val result = action match {
        case Collect => rdd.collect
        case CollectByKey => rdd.collectAsMap
        case _ => throw new IllegalArgumentException(s"Unrecognized action $action")
      }
      val tres = TestResult(tname, tname, Some(getSize(result)))
      trace(tname, s"Completed xform test $tname with result=$tres")
      tres
    }
    results
  }
  def runAggTests(name: String, rdd: AggRDD, actions: Seq[Action]): Seq[TestResult] = {
    val results = for (action <- actions) yield {
      val tname = s"$name/$action"
      trace(tname, s"Starting xform test $tname")
      val result = action match {
        case Collect => rdd.collect
        case CollectByKey => rdd.collectAsMap
        case _ => throw new IllegalArgumentException(s"Unrecognized action $action")
      }
      val tres = TestResult(tname, tname, Some(getSize(result)))
      trace(tname, s"Completed xform test $tname with result=$tres")
      tres
    }
    results
  }
  def runCountTests(name: String, rdd: CountRDD, actions: Seq[Action]): Seq[TestResult] = {
    val results = for (action <- actions) yield {
      val tname = s"$name/$action"
      trace(tname, s"Starting xform test $tname")
      val result = action match {
        case Collect => rdd.collect
        case CollectByKey => rdd.collectAsMap
        case _ => throw new IllegalArgumentException(s"Unrecognized action $action")
      }
      val tres = TestResult(tname, tname, Some(getSize(result)))
      trace(tname, s"Completed xform test $tname with result=$tres")
      tres
    }
    results
  }
}

case class TestDimRange[K](min: K, max: K)

//case class TestDim[K](@BeanProperty name: String, @BeanProperty battery: TestBattery, @BeanProperty var dimRange: TestDimRange[K], @BeanProperty var optChild: Option[TestDim] = None = {
//  optChild match {
//    case Some(child) => {
//      child.runTests(
//    }
//
//  }
//  class TestMatrix
//}

