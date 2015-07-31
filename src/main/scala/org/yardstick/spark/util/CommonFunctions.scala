package org.yardstick.spark.util

import org.apache.ignite.cache.CacheMode
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


/**
 * Created by sany on 8/7/15.
 */
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
    var igniteContext = new IgniteContext[String, Twitter](sc,
      () => new IgniteConfiguration())
    val cacheCfg = new CacheConfiguration[String, Twitter]()
    cacheCfg.setName(cacheName)
    cacheCfg.setCacheMode(CacheMode.PARTITIONED)
    cacheCfg.setIndexedTypes(classOf[String], classOf[Twitter])
    igniteContext.fromCache(cacheCfg)
  }
}
