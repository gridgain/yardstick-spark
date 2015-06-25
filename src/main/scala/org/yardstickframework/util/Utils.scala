package org.yardstickframework.util

import java.io.File

import org.apache.commons.io.FileUtils

/**
 * Created by sany on 25/6/15.
 */
class Utils {

  def deleteFileIfExists(path: String): String = {
    val file = new File(path)
    if (file.exists) {
      if (file.isDirectory)
        FileUtils.deleteDirectory(file)
      else
        FileUtils.forceDelete(file)
    }
    path
  }
}
