package org.yardstickframework.ignite

/**
 * Created by sany on 6/7/15.
 */

import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.query.{QueryCursor, SqlFieldsQuery, SqlQuery}
import org.apache.ignite.spark.{IgniteRDD, IgniteContext}
import org.apache.spark.sql.DataFrame
import org.yardstickframework._
import org.yardstickframework.ignite.util._
import org.apache.ignite.configuration._
import org.yardstickframework.spark.util.YamlConfiguration
import org.yardstickframework.util.{TimerArray, StorageFunctions}

import scala.util.Random

import org.apache.ignite.scalar.scalar._

import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache}

import collection.JavaConverters._

class IgniteSqlBenchmark extends IgniteAbstractBenchmark {

  var sqlConfig: YamlConfiguration = _
  var cache: IgniteRDD[String, Twitter] = _
  val timer = new TimerArray
  var dF: DataFrame = _

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration): Unit = {
    super.setUp(cfg)
    val configFile = cfg.customProperties.asScala
      .getOrElse("SQL_CONFIG_FILE", "config/benchmark-twitter.yml")
    sqlConfig = new YamlConfiguration(configFile)
    println(sqlConfig)
    val csvFile = sqlConfig("twitter.input.file").getOrElse("Twitter_Data.csv")
    cache = new CommonFunctions().getIgniteCacheConfig(sc)
    new CommonFunctions().loadDataInToIgniteRDD(sc, cache, csvFile, "\t")
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    val twitterSql = sqlConfig("twitter.sql",
      """SELECT created_at, COUNT(tweet) as count1 FROM Twitter
          GROUP BY created_at ORDER BY count1  limit 50""".stripMargin)
    val runResults = timer("Twitter-Data") {
      dF = new CommonFunctions().executeQuery(cache, twitterSql)
    }
    new StorageFunctions(dF).saveFileAsParquetFile(sqlConfig("twitter.output.parquetfile", "parquet-output"))
    new StorageFunctions(dF).saveFileAsTextFile(sqlConfig("twitter.output.textfile", "text-output"))
    true
  }

}

object IgniteSqlBenchmark {
  def main(args: Array[String]) {
    val b = new IgniteSqlBenchmark
    b.setUp(new BenchmarkConfiguration())
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}
