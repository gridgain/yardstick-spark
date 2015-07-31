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
package org.yardstick.spark.util

import com.google.common.hash.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.SQLContext

object LoadFunctions {

  var hashRecords = false

  val hashFunction = hashRecords match {
    case true => Some(Hashing.goodFastHash(math.max(4, 4) * 4))
    case false => None
  }

  def genStringData(sc: SparkContext, optsIndex: Int, testOpt: TestOpt) = {
    val rdd = KVDataGenerator.createKVStringDataSet(sc, testOpt.numRecords, testOpt.uniqueKeys, testOpt.keyLength, testOpt.uniqueValues,
      testOpt.valueLength, testOpt.numPartitions, testOpt.randomSeed, testOpt.persistenceType, "/tmp/ysdata", hashFunction)
    rdd
  }

  def genIntData(sc: SparkContext, optsIndex: Int, testOpt: TestOpt) = {
    val rdd = KVDataGenerator.createKVIntDataSet(sc, testOpt.numRecords, testOpt.uniqueKeys, testOpt.uniqueValues,
      testOpt.numPartitions, testOpt.randomSeed, testOpt.persistenceType, "/tmp/ysdata")
    rdd
  }

  def loadDataCSVFile(sqlContext: SQLContext, hdfsPath: String, delimiter: String): DataFrame = {
    sqlContext.load("com.databricks.spark.csv", Map("path" -> hdfsPath, "header" -> "true", "delimiter" -> delimiter))
  }

  def loadFromJSONFile(): Unit = {

  }

  def loadFromParquetFile(): Unit = {

  }

  def executeQuery(sqlContext: SQLContext, query: String): DataFrame = {
    sqlContext.sql(query)
  }
}
