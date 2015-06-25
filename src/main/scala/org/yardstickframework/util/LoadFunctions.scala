package org.yardstickframework.util

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by sany on 25/6/15.
 */
class LoadFunctions() {
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
