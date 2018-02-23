package com.paytm.WebLogChallenge

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by vijay-alagudevan on 20/2/2018.
  */

class TestWebLog extends FlatSpec with Matchers with SharedSparkContext with Checkers {

  val webLog = new WebLog()

  def getTestFilePath(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }

  def getDFfromTestResources(sqlContext: SQLContext, fileName: String): DataFrame = {

    sqlContext
      .read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",")
      .load(getTestFilePath(fileName))
  }

  "web log unit test " should " return valid results " in {

    val sqlContext = new HiveContext(new SQLContext(sc).sparkContext)
    val sampleDF  = getDFfromTestResources(sqlContext, "weblog_sample_unittest.csv")

    val resultAverageSessionDF = webLog.getAverageSessionTime(sqlContext, sampleDF)
    assert (resultAverageSessionDF.take(1)(0)(0).toString == "22387.333333333332", "-- AVERAGE SESSION TIME --")

    val resultUniqueURLVisitsDF = webLog.getUniqueURLVisits(sqlContext, sampleDF)
    val expectedOutputUniqueURLVisits = getDFfromTestResources(sqlContext, "output_unique_url_visits.csv")

    assert((resultUniqueURLVisitsDF.except(expectedOutputUniqueURLVisits).count() == 0
      && expectedOutputUniqueURLVisits.except(resultUniqueURLVisitsDF).count() == 0) == true, "-- UNIQUE URL VISITS --")

    val resultMostEngagedUsersDF = webLog.getMostEngagedUsers(sqlContext, sampleDF)
    val expectedOutputMostEngagedUsers = getDFfromTestResources(sqlContext, "output_most_engaged_users.csv")

    assert((resultMostEngagedUsersDF.except(expectedOutputMostEngagedUsers).count() == 0
      && expectedOutputMostEngagedUsers.except(resultMostEngagedUsersDF).count() == 0) == true, "-- MOST ENGAGED USERS --")

  }

}
