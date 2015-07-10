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

import org.apache.spark.storage._
import org.yardstickframework._
import org.yardstickframework.impl.BenchmarkLoader
import org.yardstickframework.spark.util.YamlConfiguration
import org.yardstickframework.util.{TimerArray, _}
import com.google.common.hash.Hashing
import org.apache.spark.sql.DataFrame
import collection.JavaConverters._

class SparkSqlQueryBenchmark extends SparkAbstractBenchmark("query") {

  val timer = new TimerArray
  var sqlConfig: YamlConfiguration = _
  var dF: DataFrame = _

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
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    val twitterSql = sqlConfig("twitter.sql",
      """SELECT created_at, COUNT(tweet) as count1 FROM Twitter
          GROUP BY created_at ORDER BY count1  limit 50""".stripMargin)
    val runResults = timer("Twitter-Data") {
//      val hashFunction = hashRecords match {
//        case true => Some(Hashing.goodFastHash(math.max(4, 4) * 4))
//        case false => None
//      }
//      val rdd=DataGenerator.createKVStringDataSet(sc, 100, 100, 4,50,
//        4, 2, 8, "memory", "/tmp/", hashFunction)
//      rdd.collect().foreach(println)
      dF= LoadFunctions.executeQuery(sqlContext, twitterSql)
    }
    true
  }
}

object SparkSqlQueryBenchmark {
  def main(args: Array[String]) {
    val b = new SparkSqlQueryBenchmark
    b.setUp(new BenchmarkConfiguration())
    b.test(new java.util.HashMap[AnyRef, AnyRef]())

  }
}


