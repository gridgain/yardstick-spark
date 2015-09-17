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
package org.yardstick.spark.util

import org.apache.ignite.cache.{CacheRebalanceMode, CacheMode}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.yardstick.spark.{TestSqlCacheConfiguration, TestCacheConfiguration}

class CommonFunctions {
  def loadDataCSVFileToObject(sc: SparkContext, hdfsPath: String, delimiter: String): RDD[(String, Twitter)] = {
    sc.textFile(hdfsPath).filter(line => line != "id\ttweet\tcreated_at\tFcount\tRcount\tGlocation\tcountry\tusername" && line.split(delimiter).length > 7).map(line => (line.split(delimiter)(0), new Twitter(line.split(delimiter)(0), line.split(delimiter)(1), line.split(delimiter)(2),
      line.split(delimiter)(3), line.split(delimiter)(4), line.split(delimiter)(5), line.split(delimiter)(6), line.split(delimiter)(7))))
  }

  def loadDataInToIgniteRDD(sc: SparkContext, igniteRdd: IgniteRDD[String, Twitter], csvFile: String, delimiter: String) {
    igniteRdd.savePairs(loadDataCSVFileToObject(sc, csvFile, delimiter))
  }

  def executeQuery(igniteRdd: IgniteRDD[String, Twitter], query: String): DataFrame = {
    igniteRdd.sql(query)
  }

  def getIgniteCacheConfig(sc: SparkContext, cacheName: String): IgniteRDD[String, Twitter] = {

    val igniteProps = System.getProperties.getProperty("ignite.properties.file",
      "file:///root/yardstick-spark/config/spark-aws-config.xml")
    println(s"Ignite properties file=$igniteProps")
    val igniteContext = new IgniteContext[String, Twitter](sc,
      () ⇒ IgnitionEx.loadConfiguration(igniteProps).get1(), false)


    val cconf = new TestSqlCacheConfiguration[String, Twitter]().cacheConfiguration(cacheName)
    println(s"Set igniteRDD Cache RebalanceMode=${cconf.getRebalanceMode}")
    val cache: IgniteRDD[String, Twitter] = igniteContext.fromCache(cconf)
   // var igniteContext = new IgniteContext[String, Twitter](sc,
    //  () => new IgniteConfiguration())
    val cacheCfg = new CacheConfiguration[String, Twitter]()
    cacheCfg.setName(cacheName)
    cacheCfg.setCacheMode(CacheMode.PARTITIONED)
    cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC)
    cacheCfg.setIndexedTypes(classOf[String], classOf[Twitter])
    igniteContext.fromCache(cacheCfg)
  }
}
