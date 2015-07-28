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

case class Entity(@ScalarCacheQuerySqlField id: Int, @ScalarCacheQuerySqlField name: String,@ScalarCacheQuerySqlField salary: Int)
abstract class SparkAbstractBenchmark[RddK,RddV](val cacheName: String)
  (implicit rddK : TypeTag[RddK], rddV: TypeTag[RddV]) extends IgniteAbstractBenchmark
                                                                 with java.io.Serializable {

  var sc: SparkContext = _
  var sqlContext: HiveContext = _

  type ScalarCacheQuerySqlField = QuerySqlField@field
  type ScalarCacheQueryTextField = QueryTextField@field

//  val PARTITIONED_CACHE_NAME = "partitioned"

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
    super.setUp(cfg)
//    var master = System.getenv("MASTER")
    // Hack to get master
    // Line is in /root/spark/conf/spark-env.sh:
    // export SPARK_MASTER_IP=ec2-184-72-155-207.compute-1.amazonaws.com
    val sfile = "/root/spark/conf/spark-env.sh"
    val master = if (new java.io.File(sfile).exists) {
      val ipLine = scala.io.Source.fromFile(sfile).getLines.toList
        .filter(l => !l.startsWith("#") && l.contains("SPARK_MASTER_IP"))
      val ip=ipLine.head.substring(ipLine.head.lastIndexOf("=") + 1)
      val portLine = scala.io.Source.fromFile(sfile).getLines.toList
        .filter(l => !l.startsWith("#") && l.contains("SPARK_MASTER_PORT"))
      val port = if (!portLine.isEmpty) {
        portLine.head.substring(portLine.head.lastIndexOf("=") + 1)
      } else {
        "7077"
      }
      s"spark://$ip:$port"
    } else {
      s"local[${Runtime.getRuntime.availableProcessors}]"
    }
    val msg = s"*** MASTER is $master ****"
    System.err.println(msg)
    tools.nsc.io.File("/tmp/MASTER.txt").writeAll(msg)
    val testName = "SparkBenchmark"  // TODO: specify Core or SQL
    val sconf = new SparkConf()
        .setAppName(testName)
        .setMaster(master)
    sc = new SparkContext(sconf)
    sc.setLocalProperty("spark.akka.askTimeout","180")
    sc.setLocalProperty("spark.driver.maxResultSize","2GB")
    sqlContext = new HiveContext(sc)
  }

  @throws(classOf[Exception])
  override def tearDown() {
    sc.stop
  }
}

object SparkAbstractBenchmark {
  val IP_FINDER = new TcpDiscoveryVmIpFinder(true)
  def igniteConfiguration[RddK,RddV](gridName: String, client: Boolean = true)
    (implicit rddK : TypeTag[RddK], rddV: TypeTag[RddV]): IgniteConfiguration = {
    val cfg = new IgniteConfiguration
    val discoSpi = new TcpDiscoverySpi
    discoSpi.setIpFinder(IP_FINDER)
    cfg.setDiscoverySpi(discoSpi)
    cfg.setCacheConfiguration(new TestCacheConfiguration[TypeTag[RddK], TypeTag[RddV]]()
      .cacheConfiguration(gridName))
    cfg.setClientMode(client)
    cfg.setGridName(gridName)
    cfg
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