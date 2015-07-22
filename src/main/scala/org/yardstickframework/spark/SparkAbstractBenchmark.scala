/*
 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.yardstickframework.spark

import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.query.annotations.{QuerySqlField, QueryTextField}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.yardstick.IgniteAbstractBenchmark
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext
import org.yardstickframework._
import org.yardstickframework.spark.YsSparkTypes.{RddKey, RddVal}

import scala.annotation.meta.field

case class IcInfo(ic: IgniteContext[RddKey, RddVal], icCache: IgniteRDD[RddKey, RddVal])
case class Entity(@ScalarCacheQuerySqlField id: Int, @ScalarCacheQuerySqlField name: String,@ScalarCacheQuerySqlField salary: Int)

object SparkAbstractBenchmark {

  val IP_FINDER = new TcpDiscoveryVmIpFinder(true)
  val PARTITIONED_CACHE_NAME = "partitioned"

  def cacheConfiguration(gridName: String): CacheConfiguration[String, Entity] = {
    val ccfg = new CacheConfiguration[String, Entity]()
    ccfg.setBackups(1)
    ccfg.setName(PARTITIONED_CACHE_NAME)
    ccfg.setCacheMode(CacheMode.PARTITIONED)
    ccfg.setIndexedTypes(classOf[String], classOf[Entity])
    ccfg
  }

  def configuration(gridName: String = "SparkGrid", client: Boolean = true): IgniteConfiguration = {
    val cfg = new IgniteConfiguration
    val discoSpi = new TcpDiscoverySpi
    discoSpi.setIpFinder(IP_FINDER)
    cfg.setDiscoverySpi(discoSpi)
    cfg.setCacheConfiguration(cacheConfiguration(gridName))
    cfg.setClientMode(client)
    cfg.setGridName(gridName)
    cfg
  }

}
abstract class SparkAbstractBenchmark(cacheName: String) extends IgniteAbstractBenchmark
                                                                 with java.io.Serializable {

  import SparkAbstractBenchmark._
  var sc: SparkContext = _
  var sqlContext: HiveContext = _

  var ic: IgniteContext[RddKey, RddVal] = _
  var icCache: IgniteRDD[RddKey, RddVal] = _

  type ScalarCacheQuerySqlField = QuerySqlField@field
  type ScalarCacheQueryTextField = QueryTextField@field

  // Following test is taken from IgniteRDDSpec in the original Ignite distribution
  def icTest(/*cfg: BenchmarkConfiguration */)   {
    val sc = new SparkContext("local[*]", "test")
    val ic = new IgniteContext[String, Entity](sc,
      () ⇒ new IgniteConfiguration())

    try {
      val cache: IgniteRDD[String, Entity] = ic.fromCache(cacheConfiguration("client"))

      import ic.sqlContext.implicits._

      cache.savePairs(sc.parallelize(0 to 1000, 2).map(i ⇒ (String.valueOf(i), new Entity(i, "name" + i, i * 100))))

      val df = cache.sql("select id, name, salary from Entity where name = ? and salary = ?", "name50", 5000)

      df.printSchema()

      val res = df.collect()

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
      sc.stop()
      ic.close()
    }
  }

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
    super.setUp(cfg)

    // Run a test using IgniteRDDSpec from the original Ignite distribution
    icTest()

    sc = new SparkContext("local[2]","itest")
    sqlContext = new HiveContext(sc)
    val ic2 = new IgniteContext[RddKey, RddVal](sc,
      () ⇒ new IgniteConfiguration())
    val icCache2 = ic2.fromCache(PARTITIONED_CACHE_NAME)
       icCache2.savePairs(  sc.parallelize({
        (0 until 1000).map{ n => (n.toLong, s"I am $n")}
          }, 10)) // .persist()
      println(icCache2.collect)

  }

  @throws(classOf[Exception])
  override def tearDown() {
    sc.stop

  }

}
