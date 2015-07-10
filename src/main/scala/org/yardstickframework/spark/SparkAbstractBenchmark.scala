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

import org.apache.ignite.cache.query.annotations.{QueryTextField, QuerySqlField}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.{IgniteRDD, IgniteContext}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.yardstickframework._
import org.apache.spark.sql.hive.HiveContext
import org.yardstickframework.spark.YsSparkTypes.{RddKey, RddVal, RddTuple}
import scala.annotation.meta.field

case class IcInfo(ic: IgniteContext[RddKey, RddVal], icCache: IgniteRDD[RddKey, RddVal])

abstract class SparkAbstractBenchmark(cacheName: String) extends BenchmarkDriverAdapter {
  var sc: SparkContext = _
  var ic: IgniteContext[RddKey, RddVal] = _
  var icCache: IgniteRDD[RddKey, RddVal] = _
  var sqlContext: HiveContext = _

  val IP_FINDER = new TcpDiscoveryVmIpFinder(true)

  /** Partitioned cache name. */
  val PARTITIONED_CACHE_NAME = "partitioned"

  /** Type alias for `QuerySqlField`. */
  type ScalarCacheQuerySqlField = QuerySqlField@field

  /** Type alias for `QueryTextField`. */
  type ScalarCacheQueryTextField = QueryTextField@field

  /**
   * Gets ignite configuration.
   *
   * @param gridName Grid name.
   * @param client Client mode flag.
   * @return Ignite configuration.
   */
  def configuration(gridName: String, client: Boolean): IgniteConfiguration = {
    val cfg = new IgniteConfiguration
    val discoSpi = new TcpDiscoverySpi
    discoSpi.setIpFinder(IP_FINDER)
    cfg.setDiscoverySpi(discoSpi)
    cfg.setCacheConfiguration(cacheConfiguration(gridName))
    cfg.setClientMode(client)
    cfg.setGridName(gridName)
    cfg
  }

  /**
   * Gets cache configuration for the given grid name.
   *
   * @param gridName Grid name.
   * @return Cache configuration.
   */
  def cacheConfiguration(gridName: String): CacheConfiguration[Object, Object] = {
    val ccfg = new CacheConfiguration[Object, Object]()
    ccfg.setBackups(1)
    ccfg.setName(PARTITIONED_CACHE_NAME)
    ccfg.setIndexedTypes(classOf[RddKey], classOf[RddVal])
    ccfg
  }

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
    super.setUp(cfg)
    sc = new SparkContext("local[4]", "BenchmarkTest")
    sqlContext = new HiveContext(sc)
    ic = new IgniteContext[RddKey, RddVal](sc,
      () ⇒ configuration("client", client = true))
    //    import ic.sqlContext.implicits._
    icCache = ic.fromCache(PARTITIONED_CACHE_NAME)

  }

  @throws(classOf[Exception])
  override def tearDown() {
    sc.stop
    ic.close
  }

}

