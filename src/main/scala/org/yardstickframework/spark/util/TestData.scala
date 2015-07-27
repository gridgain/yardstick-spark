package org.yardstickframework.spark.util

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sany on 24/7/15.
 */
case class Reditt(body:String,score_hidden:String,archived:String,name:String,author:String,author_flair_text:String,downs:String,created_utc:String,subreddit_id:String,link_id:String,parent_id:String,score:String,retrieved_on:String,controversiality:String,gilded:String,id:String,subreddit:String,ups:String,distinguished:String,author_flair_css_class:String)
object TestData {
  def main(args: Array[String]) {
    val     sc = new SparkContext("local[2]","itest")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._





    val reditt = sc.textFile("/home/sany/Desktop/2007Reditt.csv").map(_.split("\n")).take(10)
      //map(p => Reditt(p(0), p(1),p(2), p(3),p(4), p(5),p(6), p(7),p(8), p(9),p(10), p(11),p(12), p(13),p(14), p(15),p(16), p(17),p(18), p(19))).toDF()

//    reditt.registerTempTable("Reditt")


    //val teenagers = sqlContext.sql("select * from Reditt limit 100")
  //  teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }

}
