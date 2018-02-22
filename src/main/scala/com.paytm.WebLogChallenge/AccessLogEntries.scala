package com.paytm.WebLogChallenge

import java.sql.Timestamp

/**
  * Created by vijay-alagudevan on 20/2/2018.
  */

case class AccessLogEntries(
                             timestamp: Timestamp,
                             client: String,
                             request_processing_time: Float,
                             backend_processing_time: Float,
                             response_processing_time: Float,
                             request: String)