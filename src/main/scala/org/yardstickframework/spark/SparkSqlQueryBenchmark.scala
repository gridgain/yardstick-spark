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

import org.apache.spark._
import org.yardstickframework._

/**
 * Hazelcast benchmark that performs put and query operations.
 */
class SparkSqlQueryBenchmark extends SparkAbstractBenchmark("query") {
   @throws(classOf[Exception])
   override def setUp(cfg: BenchmarkConfiguration) {
     super.setUp(cfg)
   }

   @throws(classOf[java.lang.Exception])
   override def test(ctx: java.util.Map[AnyRef, AnyRef]) : Boolean =  {
      val rdd = sc.parallelize( (0 until 1000).toSeq)
      val mrdd = rdd.sum
      println(s"Is the n*(n+1)/2 formula working? $mrdd")
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


