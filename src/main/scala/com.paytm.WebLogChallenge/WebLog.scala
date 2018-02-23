package com.paytm.WebLogChallenge

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.text.SimpleDateFormat

/**
  * Created by vijay-alagudevan on 20/2/2018.
  */

class WebLog extends WebLogHelper with Serializable {

  def MAIN(args: Seq[String]): Unit = {

    lazy val (runDate, inputFilePath, sessionTime) = parseCommandLineArguments(args)

    LOGR.warn("NOTE: Read Input log file..")
    val webLogDF = getWebLogDF(inputFilePath, c_sqlContext)

    LOGR.warn("NOTE: Append Previous access time to the DF..")
    val withPreviousAccessTimeDF = getPreviousAccessTime(c_sqlContext, webLogDF)

    LOGR.warn("NOTE: Append Session Counter to the DF..")
    val withSessionCounter = getDFwithSessionCounter(c_sqlContext, withPreviousAccessTimeDF, sessionTime)

    LOGR.warn("NOTE: Sessionize the DF..")
    val sessionizedDF = getSessionizedDF(c_sqlContext, withSessionCounter)

    LOGR.warn("*********** Average Session Time ************")
    getAverageSessionTime(c_sqlContext, sessionizedDF)
      .show()

    LOGR.warn("*********** Unique URL visits per session ************")
    getUniqueURLVisits(c_sqlContext, sessionizedDF)
      .show()

    LOGR.warn("*********** Most engaged users ************")
    getMostEngagedUsers(c_sqlContext, sessionizedDF)
      .show()

    stopSparkContext()
  }

  def getWebLogDF(filePath: String, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    val regexLogSplit = " (?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val timeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"

    val rdd = sqlContext.sparkContext.textFile(filePath)

    val df = rdd
      .filter(line => line.split(regexLogSplit).length >= 15)
      .map({ log =>
        val entry = log.split(regexLogSplit)
        val sdf = new SimpleDateFormat(timeStampFormat)
        val timestamp = new Timestamp(sdf.parse(entry(0)).getTime());

        AccessLogEntries(timestamp, entry(2).split(":")(0), entry(4).toFloat, entry(5).toFloat, entry(6).toFloat, entry(11).split(" ")(1))
    })
      .toDF()

    df.withColumn("total_processing_time", ($"request_processing_time" + $"backend_processing_time" + $"response_processing_time"))
  }

  def getPreviousAccessTime(sqlContext: SQLContext, df: DataFrame): DataFrame = {

    df
      .withColumn("previous_timestamp", lag("timestamp", 1).over(spark.sql.expressions.Window.partitionBy("client").orderBy("timestamp")))
  }

  def getDFwithSessionCounter(sqlContext: SQLContext, df: DataFrame, sessionTime: Int): DataFrame = {

    import sqlContext.implicits._
    df
      .withColumn("session_counter", getSessionCounter(unix_timestamp($"timestamp"), unix_timestamp($"previous_timestamp"), lit(sessionTime)) )
  }

  def getSessionizedDF(sqlContext: SQLContext, df: DataFrame): DataFrame = {

    df
      .withColumn("session_id", concat_ws("-",  sum("session_counter").over(Window.partitionBy("client").orderBy("timestamp"))))
  }

  def getAverageSessionTime(sqlContext: SQLContext, df: DataFrame): DataFrame = {

    import sqlContext.implicits._

    df
      .agg(((unix_timestamp(max($"timestamp")) - unix_timestamp(min($"timestamp")) ) / countDistinct($"session_id"))
      .alias("average_session_time"))
  }

  def getUniqueURLVisits(sqlContext: SQLContext, df: DataFrame): DataFrame = {

    df
      .groupBy("session_id")
      .agg(countDistinct("request").alias("unique_url_visits"))
  }

  def getMostEngagedUsers(sqlContext: SQLContext, df: DataFrame): DataFrame = {

    import sqlContext.implicits._

    df
      .groupBy("client", "session_id")
      .agg((unix_timestamp(max($"timestamp")) - unix_timestamp(min($"timestamp"))).alias("most_engaged_users"))
      .orderBy(desc("most_engaged_users"))
  }

}