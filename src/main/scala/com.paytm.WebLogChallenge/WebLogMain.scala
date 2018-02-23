package com.paytm.WebLogChallenge

import org.slf4j.LoggerFactory

/**
  * Created by vijay-alagudevan on 20/2/2018.
  */

object WebLogMain extends App {
  lazy val LOGR = LoggerFactory.getLogger(getClass.getName)

  val commandLineArguments = args.toList
  LOGR.warn(s"Command line arguments = ${commandLineArguments.mkString(" ; ")}")

  val webLog = new WebLog

  webLog.initializeSparkContext(webLog.getClass.getSimpleName)
  webLog.MAIN(commandLineArguments)

  if (LOGR.isInfoEnabled)
    LOGR.info("Finished ")

}