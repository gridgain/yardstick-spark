package org.yardstickframework.spark.util

import org.apache.spark.rdd.RDD

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

/**
 * Created by sany on 27/6/15.
 */
object Operations {
  def stringTransformsTests(rdd: RDD[(String, String)], transform: String, action: String) = {
    val xformRdd : RDD[(String,String)] =
      transform match {
        case "map" =>
          rdd.map { case (k, v) => (k, s"MappedVal: $v")} // .keyBy(_._1)
        case "sortByKey" =>
          rdd.sortByKey(true)
        case _ =>
          throw new IllegalArgumentException("Str Operations.Transform error: Please pass correct arguments")

      }
    val retval = action match {
      case "take" =>
        xformRdd.take(50)
      case "countByKey" =>
        xformRdd.countByKey()
      case "collect" =>
        xformRdd.collect()
      case _ =>
        throw new IllegalArgumentException("Str Operations.Action error: Please pass correct arguments")
    }
    retval
  }

  def stringAggregationTests(rdd: RDD[(String, String)], transform: String,
    action: String) = {
      val xformRdd  =
      transform match {
          case "groupBykey" =>
          rdd.groupByKey()
      }
    val retval = action match {
      case "take" =>
        xformRdd.take(50)
      case "countByKey" =>
        xformRdd.countByKey()
      case "collect" =>
        xformRdd.collect()
      case _ =>
        throw new IllegalArgumentException("Str Operations.Action error: Please pass correct arguments")
    }
    retval
  }


  def intTransformsTests(rdd: RDD[(Int, Int)], transform: String, action: String) = {
    val returnRDD =

    transform match {
      case "map" =>
        rdd.map { case (k, v) => (k, v.toInt)}
//      case "groupBykey" =>
//        rdd.groupByKey()
      case "sortByKey" =>
        rdd.sortByKey(true)
      case _ =>
        throw new IllegalArgumentException("INT-Operations.Transform error: Please pass correct arguments")

    }
    val retval = action match {
      case "take" =>
        returnRDD.take(100).foreach(println)
      case "countByKey" =>
        returnRDD.countByKey.foreach(println)
      case "collect" =>
        returnRDD.collect().foreach(println)
      case _ =>
        throw new IllegalArgumentException("INT-Operations.Action error: Please pass correct arguments")
    }
    retval
  }

  def intAggregationTests(rdd: RDD[(Int, Int)], transform: String, action: String) = {
    val returnRDD =

    transform match {
      case "groupBykey" =>
        rdd.groupByKey()
      case _ =>
        throw new IllegalArgumentException("INT-Operations.Transform error: Please pass correct arguments")

    }
    val retval = action match {
      case "take" =>
        returnRDD.take(100).foreach(println)
      case "countByKey" =>
        returnRDD.countByKey.foreach(println)
      case "collect" =>
        returnRDD.collect().foreach(println)
      case _ =>
        throw new IllegalArgumentException("INT-Operations.Action error: Please pass correct arguments")
    }
    retval
  }
}
