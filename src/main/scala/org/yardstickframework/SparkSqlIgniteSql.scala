package org.yardstickframework

import org.apache.ignite.spark.IgniteRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.yardstickframework.ignite.util._
import org.yardstickframework.spark.util.{LoadFunctions, TimedResult, YamlConfiguration}
import org.yardstickframework.spark.{CoreTestConfig, SparkIgniteAbstractBenchmark, SqlBatteryConfigs, SqlTestMatrix}

import scala.collection.JavaConverters._


class SparkSqlIgniteSql extends SparkIgniteAbstractBenchmark {
  var coreTestsFile : String = "config/sqlTests.yml"
  var sqlConfig: YamlConfiguration = _
  var cache: IgniteRDD[String, Twitter] = _
  val timer = new TimedResult()
  var dF: DataFrame = _


  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration): Unit = {
    super.setUp(cfg)
    val configFile = cfg.customProperties.asScala
      .getOrElse("SQL_CONFIG_FILE", "config/benchmark-twitter.yml")
    sqlConfig = new YamlConfiguration(configFile)
    println(sqlConfig)
       val  csvFile = sqlConfig("twitter.input.file").getOrElse("Twitter_Data.csv")
      cache = new CommonFunctions().getIgniteCacheConfig(sc)
   // new CommonFunctions().loadDataInToIgniteRDD(sc, cache, csvFile, "\t")

    //val df = LoadFunctions.loadDataCSVFile(sqlContext, csvFile, "\t")
   // df.registerTempTable("Twitter")
   // df.persist(StorageLevel.MEMORY_ONLY)
  }

  def readTestConfig(ymlFile: String) = {
    val yml = new YamlConfiguration(ymlFile)

    def toStrList(cval: Option[_], default: Seq[String]) : Seq[String] = {
      import collection.JavaConverters._
      if (cval.isEmpty) {
        default
      } else cval.get match {

        case ints: java.util.ArrayList[_] => ints.asScala.toSeq.asInstanceOf[Seq[String]]
        case _ => throw new IllegalArgumentException(s"Unexpected type in toStringList ${cval.get.getClass.getName}")
      }
    }

    def toBoolList(cval: Option[_], default: Seq[Boolean]) : Seq[Boolean] = {
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
      yml("sql.useIgnite").getOrElse(true).asInstanceOf[Boolean],
      toStrList(yml("sql.Differentfiles"),Seq("small_Twitter_Data.csv","medium_Twitter_Data.csv","large_Twitter_Data.csv"))
    )
    println(s"SQLTest config is ${conf.toString}")
    conf
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    val sqlBatteryConfigs=readTestConfig(coreTestsFile)
    timer("Twitter-Data-IgniteSQL") {
      SqlTestMatrix.runMatrix(sqlBatteryConfigs)
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
