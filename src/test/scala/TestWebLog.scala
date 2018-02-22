package com.paytm.WebLogChallenge

import com.holdenkarau.spark.testing.SharedSparkContext
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by vijay-alagudevan on 20/2/2018.
  */

class TestWebLog extends FlatSpec with Matchers with SharedSparkContext with Checkers {


  val webLog = new WebLog()

  def getTestFilePath(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }

  "getAverageSessionTime" should " return valid dataframe " in {
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sqlContext.sparkContext)

    val df = hiveContext.read.format("com.databricks.spark.csv").load(getTestFilePath("averagesession_test.csv"))
    assert ( webLog.getAverageSessionTime(sqlContext, df) == 1, "test")
  }
}
