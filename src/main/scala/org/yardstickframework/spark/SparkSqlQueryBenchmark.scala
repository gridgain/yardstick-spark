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

import com.yardstickframework.model.Sensor
import org.yardstickframework._
import com.yardstickframework.util.TimerArray


class SparkSqlQueryBenchmark extends SparkAbstractBenchmark("query") {

  val timer = new TimerArray

   @throws(classOf[Exception])
   override def setUp(cfg: BenchmarkConfiguration) {
     super.setUp(cfg)
   }

   @throws(classOf[java.lang.Exception])
   override def test(ctx: java.util.Map[AnyRef, AnyRef]) : Boolean =  {
     val rdd = sc.textFile("/home/sany/Downloads/TruckEvents.csv")
     val runResults = timer("Sensor-Data") {
       val mrdd = rdd.map(line=>new Sensor(line.split(",")(0).toInt,line.split(",")(1).toInt,line.split(",")(2).toDouble,
         line.split(",")(3).toDouble,line.split(",")(4).toInt,line.split(",")(5).toInt,line.split(",")(6).toInt,line.split(",")(7).toInt,
         line.split(",")(8).toInt,line.split(",")(9).toInt))
       println(mrdd.count())
     }
   //  println(s"Runner finished. Got results for ${runResults} features:")
     //runResults foreach { runner =>
     //  println(runner.mkString(", "))
     //}
    // println(timer.toString("Running time: "))
   //  runResults


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


