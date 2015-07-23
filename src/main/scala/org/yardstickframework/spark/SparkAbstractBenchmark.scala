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
import scala.reflect.runtime.universe._

//case class IcInfo(ic: IgniteContext[RddKey, RddVal], icCache: IgniteRDD[String, Entity])
case class IcInfo(ic: IgniteContext[RddKey, RddVal], icCache: IgniteRDD[Long, String])
case class Entity(@ScalarCacheQuerySqlField id: Int, @ScalarCacheQuerySqlField name: String,@ScalarCacheQuerySqlField salary: Int)

abstract class SparkAbstractBenchmark[RddK,RddV](cacheName: String)
  (implicit rddK : TypeTag[RddK], rddV: TypeTag[RddV]) extends IgniteAbstractBenchmark
                                                                 with java.io.Serializable {

  var sc: SparkContext = _
  var sqlContext: HiveContext = _

  var ic: IgniteContext[RddK, RddV] = _
  var icCache: IgniteRDD[RddK, RddV] = _

  type ScalarCacheQuerySqlField = QuerySqlField@field
  type ScalarCacheQueryTextField = QueryTextField@field

  val IP_FINDER = new TcpDiscoveryVmIpFinder(true)
  val PARTITIONED_CACHE_NAME = "partitioned"
  val CORE_CACHE_NAME = "core"

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
    super.setUp(cfg)
    sc = new SparkContext("local[2]","itest")
    sqlContext = new HiveContext(sc)
     ic = new IgniteContext[RddK, RddV](sc,
      () ⇒ configuration(cacheName))
    icCache = ic.fromCache(cacheName)
    super.setUp(cfg)
  }

  def configuration(gridName: String = "SparkGrid", client: Boolean = true): IgniteConfiguration = {
    val cfg = new IgniteConfiguration
    val discoSpi = new TcpDiscoverySpi
    discoSpi.setIpFinder(IP_FINDER)
    cfg.setDiscoverySpi(discoSpi)
    cfg.setCacheConfiguration(new TestCacheConfiguration[RddK, RddV]()
      .cacheConfiguration(gridName))
    cfg.setClientMode(client)
    cfg.setGridName(gridName)
    cfg
  }


  @throws(classOf[Exception])
  override def tearDown() {
    sc.stop
  }
}

class TestCacheConfiguration[RddK,RddV] {
  def cacheConfiguration(gridName: String)
  (implicit rddK : TypeTag[RddK], rddV: TypeTag[RddV]): CacheConfiguration[RddK, RddV] = {
    val ccfg = new CacheConfiguration[RddK, RddV]()
    ccfg.setBackups(1)
    ccfg.setName(gridName)
    ccfg.setCacheMode(CacheMode.PARTITIONED)
    ccfg.setIndexedTypes(rddK.mirror.runtimeClass(rddK.tpe.typeSymbol.asClass),
      rddV.mirror.runtimeClass(rddV.tpe.typeSymbol.asClass))
    ccfg
  }
}