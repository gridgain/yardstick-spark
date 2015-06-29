package org.yardstickframework.util

import org.apache.spark.rdd.RDD

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
