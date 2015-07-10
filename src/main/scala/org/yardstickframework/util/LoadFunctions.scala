package org.yardstickframework.util

import com.google.common.hash.Hashing
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.yardstickframework.util.TestOpt

/**
 * Created by sany on 25/6/15.
 */
object LoadFunctions {

  var hashRecords = false

  val hashFunction = hashRecords match {
    case true => Some(Hashing.goodFastHash(math.max(4, 4) * 4))
    case false => None
  }

  def genStringData(sc: SparkContext, optsIndex: Int, testOpt: TestOpt) = {
    val rdd = DataGenerator.createKVStringDataSet(sc, testOpt.numRecords, testOpt.uniqueKeys, testOpt.keyLength, testOpt.uniqueValues,
      testOpt.valueLength, testOpt.numPartitions, testOpt.randomSeed, testOpt.persistenceType, "/tmp/ysdata", hashFunction)
    rdd
  }

  def genIntData(sc: SparkContext, optsIndex: Int, testOpt: TestOpt) = {
    val rdd = DataGenerator.createKVIntDataSet(sc, testOpt.numRecords, testOpt.uniqueKeys, testOpt.uniqueValues,
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

  def executeQuery(sqlContext: HiveContext, query: String): DataFrame = {
    sqlContext.sql(query)
  }
}
