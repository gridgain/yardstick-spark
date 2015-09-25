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
package org.yardstick.spark

import org.apache.ignite.Ignition
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.yardstickframework._

abstract class SparkIgniteAbstractBenchmark extends BenchmarkDriverAdapter {
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  @throws(classOf[Exception])
  override def setUp(cfg: BenchmarkConfiguration) {
//    super.setUp(cfg)
    val ignition  = Ignition.start("config/example-cache.xml")
    sc = new SparkContext("local[2]","itest")
    sqlContext = new SQLContext(sc)

  }

  @throws(classOf[Exception])
  override def tearDown() {
    sc.stop

  }
}

