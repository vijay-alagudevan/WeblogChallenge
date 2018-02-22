package com.paytm.WebLogChallenge

import org.apache.spark.sql.functions.udf

/**
  * Created by vijay-alagudevan on 20/2/2018.
  */

trait WebLogHelper extends AppBase {

  def parseCommandLineArguments(argsList: Seq[String]): (String, String, Int) = {

    (argsList(0), new java.io.File(".").getCanonicalPath + argsList(1), argsList(2).toInt)
  }

  val getSessionCounter = udf {
    (start_timestamp: Long, previous_timestamp: Long, sessionTime: Int) => {

      if (previous_timestamp == null)
        1
      else if ( (start_timestamp - previous_timestamp) > sessionTime*60)
        1
      else
        0
    }
  }
}
