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
package org.yardstickframework.ignite

import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.query.{QueryCursor, SqlFieldsQuery, SqlQuery}
import org.apache.ignite.spark.{IgniteRDD, IgniteContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.yardstickframework._
import org.yardstickframework.ignite.util._
import org.apache.ignite.configuration._
import org.yardstickframework.spark.{SparkIgniteAbstractBenchmark, SqlBatteryConfigs, SqlTestMatrix, SqlBattery}
import org.yardstickframework.spark.util.YamlConfiguration
import org.yardstickframework.spark.util.{TimerArray, StorageFunctions}

import scala.util.Random

import org.apache.ignite.scalar.scalar._

import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache}

import collection.JavaConverters._

class SparkIgniteSqlBenchmark extends SparkIgniteAbstractBenchmark {

  var sqlConfig: YamlConfiguration = _
  var cache: IgniteRDD[String, Twitter] = _
  val timer = new TimerArray()
  var dF: DataFrame = _


  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration): Unit = {
    super.setUp(cfg)
    val configFile = cfg.customProperties.asScala
      .getOrElse("SQL_CONFIG_FILE", "/mnt/thirdeye/yardstick-spark/config/benchmark-twitter.yml")
    sqlConfig = new YamlConfiguration(configFile)
    println(sqlConfig)
    val csvFile = sqlConfig("twitter.input.file").getOrElse("Twitter_Data.csv")
    cache = new CommonFunctions().getIgniteCacheConfig(sc)
    new CommonFunctions().loadDataInToIgniteRDD(sc, cache, csvFile, "\t")
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {

   // cache.sql(sqlConfig("twitter.sql.orderby",
   ///   """SELECT created_at, COUNT(tweet) as count1 FROM Twitter GROUP BY created_at ORDER BY count1  limit 50""".stripMargin))
    val runResults = timer("Twitter-Data-IgniteSQL") {
      SqlTestMatrix.runMatrix(SqlBatteryConfigs(cache,sqlContext,sqlConfig,true))
    }
    true
  }

}

object SparkIgniteSqlBenchmark {
  def main(args: Array[String]) {
    val b = new SparkIgniteSqlBenchmark
    b.setUp(new BenchmarkConfiguration())
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}
