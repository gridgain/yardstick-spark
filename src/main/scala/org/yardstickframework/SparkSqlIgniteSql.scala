package org.yardstickframework

import org.apache.ignite.spark.{IgniteRDD}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.yardstickframework.ignite.util._
import org.yardstickframework.spark.{SparkIgniteAbstractBenchmark, SqlBatteryConfigs, SqlTestMatrix}
import org.yardstickframework.spark.util.{LoadFunctions, YamlConfiguration, TimerArray}
import collection.JavaConverters._

class SparkSqlIgniteSql extends SparkIgniteAbstractBenchmark {

  var sqlConfig: YamlConfiguration = _
  var cache: IgniteRDD[String, Twitter] = _
  val timer = new TimerArray(cfg)
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

    val df = LoadFunctions.loadDataCSVFile(sqlContext, csvFile, "\t")
    df.registerTempTable("Twitter")
    df.persist(StorageLevel.MEMORY_ONLY)
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {

    timer("Twitter-Data-IgniteSQL") {
      SqlTestMatrix.runMatrix(SqlBatteryConfigs(cache,sqlContext,sqlConfig,true))
    }
    timer("Twitter-Data-SparkSQL") {
      SqlTestMatrix.runMatrix(SqlBatteryConfigs(cache,sqlContext,sqlConfig,false))
    }
    true
  }

}

object SparkSqlIgniteSql {
  def main(args: Array[String]) {
    val b = new SparkSqlIgniteSql
    b.setUp(new BenchmarkConfiguration())
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}
