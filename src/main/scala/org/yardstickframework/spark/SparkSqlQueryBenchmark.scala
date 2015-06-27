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
import org.yardstickframework.util.{TimerArray, _}
import com.google.common.hash.Hashing

class SparkSqlQueryBenchmark extends SparkAbstractBenchmark("query") {

  val timer = new TimerArray


  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
    super.setUp(cfg)
    val sQLContext = sqlContext
    val df = new LoadFunctions().loadDataCSVFile(sQLContext, "/home/sany/Downloads/Twitter_Data.csv", "\t")
    df.registerTempTable("Twitter")
    df.persist(StorageLevel.MEMORY_ONLY)
  }

  @throws(classOf[java.lang.Exception])
  override def test(ctx: java.util.Map[AnyRef, AnyRef]): Boolean = {
    val runResults = timer("Sensor-Data") {
    //  val dF = new LoadFunctions().executeQuery(sqlContext, "SELECT created_at, COUNT(tweet) as count1 FROM Twitter GROUP BY created_at ORDER BY count1  limit 50")
      //new StorageFunctions(dF).savePathMapParquetFile("/home/sany/Downloads/Twitter.pq")
      var hashRecords = false
      val hashFunction = hashRecords match {
        case true => Some(Hashing.goodFastHash(math.max(4, 4) * 4))
        case false => None
      }
      val rdd=DataGenerator.createKVStringDataSet(sc, 100, 100, 4,50,
        4, 2, 8, "memory", "/tmp/", hashFunction)
      rdd.collect().foreach(println)
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


