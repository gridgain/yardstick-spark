package org.yardstickframework.spark

/**
 * Created by sany on 21/7/15.
 */


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.ignite.spark.{IgniteRDD, IgniteContext}
import org.apache.spark.sql.hive.HiveContext
import org.yardstickframework.spark.util.YamlConfiguration
import org.yardstickframework.spark.YsSparkTypes.Action
import org.yardstickframework.spark.YsSparkTypes._
import org.yardstickframework.spark.util.YardstickLogger._

import scala.collection.mutable

case class SqlBatteryConfigs(ic: IgniteRDD[DataFrameKey, DataFrameVal], sQLContext: HiveContext, sqlConfig: YamlConfiguration, useIgnite: Boolean)

object SqlTestMatrix {
  val A = Array

  def runMatrix(sqlBatteryConfigs: SqlBatteryConfigs) = {

    val passArr = mutable.ArrayBuffer[Boolean]()
    val resArr = mutable.ArrayBuffer[TestResult]()
    val dtf = new SimpleDateFormat("MMdd-hhmmss").format(new Date)


      val rawname = "CoreSQLSmoke"
      val tname = s"$dtf/$rawname"
      val igniteOrNative = if (sqlBatteryConfigs.useIgnite) "igniteSQL" else "nativeSQL"
      val name = s"$tname ${igniteOrNative}"
      val dir = name.replace(" ", "/")

      val optIcInfo = if (sqlBatteryConfigs.useIgnite) Some(sqlBatteryConfigs.useIgnite) else None


      val battery = new SqlBattery(sqlBatteryConfigs, name, dir)
      val (pass, tresults) = battery.runBattery()
      passArr += pass
      resArr ++= tresults

    (passArr.forall(identity), resArr)
  }
}

object SqlBattery {

  def runBattery(): Unit = {

  }

  def main(args: Array[String]) {

  }
}

class SqlBattery(sqlBatteryConfigs: SqlBatteryConfigs,
                 testName: String, outputDir: String) extends TestBattery("SqlBattery", s"$outputDir/$testName") {
  assert(testName != null, "Hey null's are not cool")

  override def runBattery() = {

    val xformRdds = if (sqlBatteryConfigs.useIgnite) {
      Seq(
        (s"$testName/COUNT", sqlBatteryConfigs.ic.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.count",
          """COUNT(*)""".stripMargin))),
        (s"$testName/ORDERBY", sqlBatteryConfigs.ic.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.orderby",
          """ORDER BY""".stripMargin))),
        (s"$testName/GROUPBY", sqlBatteryConfigs.ic.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.groupby",
          """GROUP BY""".stripMargin))),
        (s"$testName/JOIN", sqlBatteryConfigs.ic.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.join",
          """JOIN TWO TABLES""".stripMargin))))
    } else {
      Seq(
        (s"$testName/COUNT", sqlBatteryConfigs.sQLContext.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.count",
          """COUNT(*)""".stripMargin))),
        (s"$testName/ORDERBY", sqlBatteryConfigs.sQLContext.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.orderby",
          """ORDER BY""".stripMargin))),
        (s"$testName/GROUPBY", sqlBatteryConfigs.sQLContext.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.groupby",
          """GROUP BY""".stripMargin))),
        (s"$testName/JOIN", sqlBatteryConfigs.sQLContext.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.join",
          """JOIN TWO TABLES""".stripMargin))))
    }
    val actions = Seq(SqlCollect, SqlCollectAsList)
    val res = for ((name, rdd) <- xformRdds) yield {
      runXformTests(name, rdd, actions)
    }



    // TODO: determine a pass/fail instead of returning true
    (true, (res).flatten)
  }

  def getSize(x: Any) = {
    x match {
      case arr: Array[_] => arr.length
      case l:  java.util.List[_] => l.size()
      case _ => throw new IllegalArgumentException(s"What is our type?? ${x.getClass.getName}")
    }
  }

  def runXformTests(name: String, dataFrame: InputDataFrame, actions: Seq[Action]): Seq[TestResult] = {
    val results = for (action <- actions) yield {
      val tname = s"$name/$action"
      trace(tname, s"Starting xform test $tname")
      val result = action match {
        case SqlCollect => dataFrame.collect
        case SqlCollectAsList=> dataFrame.collectAsList()
        case _ => throw new IllegalArgumentException(s"Unrecognized action $action")
      }
      val tres = TestResult(tname, tname, Some(getSize(result)))
      trace(tname, s"Completed xform test $tname with result=$tres")
      tres
    }
    results
  }
}



