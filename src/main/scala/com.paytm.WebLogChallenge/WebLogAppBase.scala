package com.paytm.WebLogChallenge

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

trait WebLogAppBase {

  @transient lazy val LOGR = LoggerFactory.getLogger(getClass.getName)

  var c_sc: SparkContext = _
  var c_sqlContext: SQLContext = _

  def initializeSparkContext(a_appName: String, mode:String="") = {

    val conf = new SparkConf()
    conf.setAppName(a_appName)
    conf.setMaster(s"local[*]")

    c_sc = new SparkContext(conf)
    c_sqlContext = new SQLContext(c_sc)
  }

  def parseCommandLineArguments(argsList: Seq[String]): (String, String) = {

    (argsList(0), new java.io.File(".").getCanonicalPath + argsList(1))
  }
}