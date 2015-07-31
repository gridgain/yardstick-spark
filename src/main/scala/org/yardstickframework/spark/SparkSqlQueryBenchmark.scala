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


import org.apache.ignite.spark.IgniteRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.yardstickframework._
import org.yardstickframework.ignite.util._
import org.yardstickframework.impl.BenchmarkLoader
import org.yardstickframework.spark.SparkSqlQueryBenchmark._
import org.yardstickframework.spark.util.{LoadFunctions, TimedResult, YamlConfiguration}

import scala.collection.JavaConverters._

class SparkSqlQueryBenchmark extends SparkAbstractBenchmark(SQL_CACHE_NAME) {

  var timer: TimedResult = _
  var sqlConfig: YamlConfiguration = _
  var dF: DataFrame = _
  var cache: IgniteRDD[String, Twitter] = _

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
    super.setUp(cfg)
    new BenchmarkLoader().initialize(cfg)
    val configFile = cfg.customProperties.asScala
      .getOrElse("SQL_CONFIG_FILE", "config/benchmark-twitter.yml")
    sqlConfig = new YamlConfiguration(configFile)
    println(sqlConfig)
    val csvFile = sqlConfig("twitter.input.file").getOrElse("Twitter_Data.csv")
    val df = LoadFunctions.loadDataCSVFile(sqlContext, csvFile, "\t")
    df.registerTempTable("Twitter")
    df.persist(StorageLevel.MEMORY_ONLY)
    timer = new TimedResult()
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    timer("Twitter-Data-SparkSQL") {
      //SqlTestMatrix.runMatrix(SqlBatteryConfigs(cache,sqlContext,sqlConfig,false))
    }
    true
  }
}

object SparkSqlQueryBenchmark {
  val SQL_CACHE_NAME = "query"
  def main(args: Array[String]) {
    val b = new SparkSqlQueryBenchmark
    b.setUp(new BenchmarkConfiguration())
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}


