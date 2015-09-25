package org.yardstick.spark

import org.apache.ignite.spark.IgniteRDD
import org.apache.spark.sql.DataFrame
import org.yardstick.spark.SparkSqlIgniteSql._
import org.yardstick.spark.util.{CommonFunctions, TimedResult, Twitter, YamlConfiguration}
import org.yardstickframework.BenchmarkConfiguration

import scala.collection.JavaConverters._

class SparkSqlIgniteSql extends SparkAbstractBenchmark(SQL_CACHE_NAME) {
  var coreTestsFile: String = "config/sqlTests.yml"
  var sqlConfig: YamlConfiguration = _
  var cache: IgniteRDD[String, Twitter] = _

  var dF: DataFrame = _

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration): Unit = {
    println(s"setUp BenchmarkConfiguration=${cfg.toString}")
    super.setUp(cfg)
    val configs = cfg.customProperties.asScala
    val configFile = "config/benchmark-twitter.yml"
    sqlConfig = new YamlConfiguration(configFile)
    println(sqlConfig)
    val csvFile = sqlConfig("twitter.input.file").getOrElse("Twitter_Data.csv")
    cache = new CommonFunctions().getIgniteCacheConfig(sc, cacheName)

  }

  def readTestConfig(ymlFile: String): SqlBatteryConfigs = {
    val yml = new YamlConfiguration(ymlFile)
    def toStrList(cval: Option[_], default: Seq[String]): Seq[String] = {
      import collection.JavaConverters._
      if (cval.isEmpty) {
        default
      } else cval.get match {
        case ints: java.util.ArrayList[_] => ints.asScala.toSeq.asInstanceOf[Seq[String]]
        case _ => throw new IllegalArgumentException(s"Unexpected type in toStringList ${cval.get.getClass.getName}")
      }
    }

    def toBoolList(cval: Option[_], default: Seq[Boolean]): Seq[Boolean] = {
      import collection.JavaConverters._
      if (cval.isEmpty) {
        default
      } else cval.get match {
        case bools: java.util.ArrayList[_] => bools.asScala.toSeq.asInstanceOf[Seq[Boolean]]
        case _ => throw new IllegalArgumentException(s"Unexpected type in toBooleanList ${cval.get.getClass.getName}")
      }
    }

    def toLong(cval: Option[Long], default: Long) = cval.getOrElse(default)
    val A = Array
    val conf = SqlBatteryConfigs(
      cache,
      sqlContext,
      sqlConfig,
      toBoolList(yml("core.useIgnite"), Seq(true, false)),
      toStrList(yml("sql.Differentfiles"), Seq("small_Twitter_Data.csv", "medium_Twitter_Data.csv", "large_Twitter_Data.csv"))
    )
    println(s"SQLTest config is ${conf.toString}")
    conf
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    val sqlBatteryConfigs = readTestConfig(coreTestsFile)
    SqlTestMatrix.runMatrix(sqlBatteryConfigs)
    true
  }

}

object SparkSqlIgniteSql {
  val SQL_CACHE_NAME = "sqlcache"

  def main(args: Array[String]) {
    val cfg = new BenchmarkConfiguration()
    cfg.commandLineArguments(args)
    val b = new SparkSqlIgniteSql
    b.setUp(cfg)
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}
