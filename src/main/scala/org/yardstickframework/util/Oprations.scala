package org.yardstickframework.util

import org.apache.spark.rdd.RDD

//

/**
 * Created by sany on 27/6/15.
 */
class Oprations {
  def callOperationTest(rdd: RDD[_], tarnsform: String, action: String, datatype: String): Unit = {
    var returnRDD: RDD[_] = null
    datatype match {
      case "string" =>
        tarnsform match {
          case "map" =>
            returnRDD = rdd.asInstanceOf[RDD[(String, String)]].map { case (k, v) => (k, v.toInt) }
          case "groupBykey" =>
            returnRDD = rdd.asInstanceOf[RDD[(String, String)]].groupByKey()
          case "sortByKey" =>
            returnRDD = rdd.asInstanceOf[RDD[(String, String)]].sortByKey(true)
          case _ => println("Please pass correct arguments")

        }
        action match {
          case "take" =>
            returnRDD.asInstanceOf[RDD[(String, String)]].take(50).foreach(println)
          case "countByKey" =>
            returnRDD.asInstanceOf[RDD[(String, String)]].countByKey.foreach(println)
          case "collect" =>
            returnRDD.collect().foreach(println)
          case _ =>
            println("Please pass correct arguments")
        }
      case "int" =>
        tarnsform match {
          case "map" =>
            returnRDD = rdd.asInstanceOf[RDD[(Int, Int)]].map { case (k, v) => (k, v.toInt) }

          case "groupBykey" =>
            returnRDD = rdd.asInstanceOf[RDD[(Int, Int)]].groupByKey()
          case "sortByKey" =>

            returnRDD = rdd.asInstanceOf[RDD[(Int, Int)]].sortByKey(true)
          case _ => println("Please pass correct arguments")

        }
        action match {
          case "take" =>
            returnRDD.asInstanceOf[RDD[(Int, Int)]].take(100).foreach(println)
          case "countByKey" =>
            returnRDD.asInstanceOf[RDD[(Int, Int)]].countByKey.foreach(println)
          case "collect" =>
            returnRDD.collect().foreach(println)
          case _ =>
            println("Please pass correct arguments")
        }
    }

  }
}
