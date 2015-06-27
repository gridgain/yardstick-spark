package org.yardstickframework.util

/**
 * Created by sany on 27/6/15.
 */
case class TestOpt(dataType:String,name: String, numRecords: Int, uniqueKeys: Int, keyLength: Int, uniqueValues: Int, valueLength: Int,
                   numPartitions: Int, randomSeed: Int, persistenceType: String)
