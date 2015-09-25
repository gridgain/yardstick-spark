package org.yardstick.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.ignite.spark.IgniteRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.yardstick.spark.YsSparkTypes.{Action, Collect, CollectList, _}
import org.yardstick.spark.util.YardstickLogger._
import org.yardstick.spark.util.{TimedResult, CommonFunctions, LoadFunctions, YamlConfiguration}

import scala.collection.mutable

case class SqlBatteryConfigs(ic: IgniteRDD[DataFrameKey, DataFrameVal], sqlContext: SQLContext, sqlConfig: YamlConfiguration, useIgnite: Seq[Boolean], fileType: Seq[String])

object SqlTestMatrix {
  val A = Array

  def runMatrix(sqlBatteryConfigs: SqlBatteryConfigs) = {

    val passArr = mutable.ArrayBuffer[Boolean]()
    val resArr = mutable.ArrayBuffer[TestResult]()
    val dtf = new SimpleDateFormat("MMdd-hhmmss").format(new Date)

    for (useIgnite <- sqlBatteryConfigs.useIgnite;
         file <- sqlBatteryConfigs.fileType) {
      val rawname = "SQLSmoke"
      val tname = s"$dtf/$rawname"
      val igniteOrNative = if (useIgnite) "ignite" else "native"
      val name = s"$tname/${file.split("\t")(1)}/${igniteOrNative}"
      val dir = name.replace(" ", "/")
      loadData(sqlBatteryConfigs, file.split("\t")(0))
      val battery = new SqlBattery(sqlBatteryConfigs, name, dir, useIgnite)
      val (pass, tresults) = battery.runBattery()
      passArr += pass
      resArr ++= tresults
    }
    (passArr.forall(identity), resArr)
  }

  def loadData(sqlBatteryConfigs: SqlBatteryConfigs, fileName: String) {

    new CommonFunctions().loadDataInToIgniteRDD(sqlBatteryConfigs.sqlContext.sparkContext, sqlBatteryConfigs.ic, fileName, "\t")

    val df = LoadFunctions.loadDataCSVFile(sqlBatteryConfigs.sqlContext, fileName, "\t")
    df.registerTempTable("Twitter")
    df.persist(StorageLevel.MEMORY_ONLY)
  }
}

class SqlBattery(sqlBatteryConfigs: SqlBatteryConfigs,
                 testName: String, outputDir: String, useIgnite: Boolean) extends TestBattery("SqlBattery", s"${outputDir}/$testName") {
  assert(testName != null, "Hey null's are not cool")

  override def runBattery() = {

    val xformRdds = if (useIgnite) {
      Seq(
        TimedResult(s"$testName/COUNT/COUNT_QrunTime") {
          (s"$testName/COUNT", sqlBatteryConfigs.ic.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.count",
            """SELECT COUNT(*) from Twitter""".stripMargin)))
        },
        TimedResult(s"$testName/ORDERBY/ORDERBY_QrunTime") {
          (s"$testName/ORDERBY", sqlBatteryConfigs.ic.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.orderby",
            """SELECT created_at, COUNT(tweet) as count1 FROM Twitter GROUP BY created_at ORDER BY count1  limit 50""".stripMargin)))
        },
        TimedResult(s"$testName/GROUPBY/GROUPBY_QrunTime") {
          (s"$testName/GROUPBY", sqlBatteryConfigs.ic.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.groupby",
            """SELECT created_at, COUNT(tweet) as count1 FROM Twitter GROUP BY created_at  limit 50""".stripMargin)))
        },
        TimedResult(s"$testName/JOIN/JOIN_QrunTime") {
          (s"$testName/JOIN", sqlBatteryConfigs.ic.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.join",
            """SELECT e.username AS userName, m.tweet AS tweetText FROM Twitter e INNER JOIN Twitter m ON e.id = m.id limit 50"""".stripMargin)))
        }
      )
    } else {
      Seq(
        TimedResult(s"$testName/COUNT/COUNT_QrunTime") {
          (s"$testName/COUNT", sqlBatteryConfigs.sqlContext.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.count",
            """SELECT COUNT(*) from Twitter""".stripMargin)))
        },
        TimedResult(s"$testName/ORDERBY/ORDERBY_QrunTime") {
          (s"$testName/ORDERBY", sqlBatteryConfigs.sqlContext.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.orderby",
            """SELECT created_at, COUNT(tweet) as count1 FROM Twitter GROUP BY created_at ORDER BY count1  limit 50""".stripMargin)))
        },
        TimedResult(s"$testName/GROUPBY/GROUPBY_QrunTime") {
          (s"$testName/GROUPBY", sqlBatteryConfigs.sqlContext.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.groupby",
            """SELECT created_at, COUNT(tweet) as count1 FROM Twitter GROUP BY created_at  limit 50""".stripMargin)))
        },
        TimedResult(s"$testName/JOIN/JOIN_QrunTime") {
          (s"$testName/JOIN", sqlBatteryConfigs.sqlContext.sql(sqlBatteryConfigs.sqlConfig("twitter.sql.join",
            """  SELECT e.username AS userName, m.tweet AS tweetText FROM Twitter e INNER JOIN Twitter m ON e.id = m.id limit 50""".stripMargin)))
        }
      )
    }
    val actions = Seq(Collect, CollectList)
    val res = for ((name, rdd) <- xformRdds) yield {
      runXformTests(name, rdd, actions)
    }

    // TODO: determine a pass/fail instead of returning true
    (true, (res).flatten)
  }

  def getSize(x: Any) = {
    x match {
      case arr: Array[_] => arr.length
      case l: java.util.List[_] => l.size()
      case _ => throw new IllegalArgumentException(s"What is our type?? ${x.getClass.getName}")
    }
  }

  def runXformTests(name: String, dataFrame: InputDataFrame, actions: Seq[Action]): Seq[TestResult] = {
    val results = for (action <- actions) yield {
      val tname = s"$name/$action"
      TimedResult(tname) {
        val result = action match {
          case Collect => dataFrame.collect
          case CollectList => dataFrame.collectAsList()
          case _ => throw new IllegalArgumentException(s"Unrecognized action $action")
        }
        TestResult(tname, tname, Some(getSize(result)))
      }
    }
    results
  }
}



