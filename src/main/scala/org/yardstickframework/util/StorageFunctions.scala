package org.yardstickframework.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Created by sany on 25/6/15.
 */

class StorageFunctions(self: DataFrame) extends Utils {


  def baseSchema: StructType = self.schema

  def savePathMapParquetFile(fullPath: String): String = {
    self.saveAsParquetFile(deleteFileIfExists(fullPath))
    fullPath
  }


}
