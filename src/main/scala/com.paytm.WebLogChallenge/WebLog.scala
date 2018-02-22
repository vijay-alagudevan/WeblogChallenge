package com.paytm.WebLogChallenge

import org.apache.spark.sql.{DataFrame, SQLContext}

class WebLog extends WebLogAppBase with Serializable {

  def MAIN(args: Seq[String]): Unit = {

    lazy val (runDate, inputFilePath) = parseCommandLineArguments(args)

    val sessionedDF = sessionizeLog(inputFilePath, c_sqlContext)

    sessionedDF.show
    /*getAverageSessionTime(sessionedDF).show()

    getUniqueURLVisits(sessionedDF).show()

    getMostEngagedUsers(sessionedDF).show()*/

  }

  def sessionizeLog(filePath: String, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    val regexLogSplit = " (?=([^\"]*\"[^\"]*\")*[^\"]*$)"

    val rdd = sqlContext.sparkContext.textFile(filePath)

    rdd
      .filter(line => line.split(regexLogSplit).length >= 15)
      .map({ log =>
      val entry = log.split(regexLogSplit)
        AccessLogEntries(entry(0), entry(2).split(":")(0), entry(4), entry(5), entry(6), entry(11).split(" ")(1))
    })
      .toDF()
  }
}