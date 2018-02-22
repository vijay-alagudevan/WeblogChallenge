package com.paytm.WebLogChallenge

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by vijay-alagudevan on 20/2/2018.
  */

trait AppBase {

  @transient lazy val LOGR = LoggerFactory.getLogger(getClass.getName)

  var c_sc: SparkContext = _
  var c_sqlContext: SQLContext = _

  def initializeSparkContext(a_appName: String, mode:String=""): Unit = {

    val conf = new SparkConf()
    conf.setAppName(a_appName)
    conf.setMaster(s"local[*]")

    c_sc = new SparkContext(conf)
    c_sqlContext = new HiveContext(c_sc)
  }

  def stopSparkContext(): Unit = {

    c_sc.stop()
  }
}