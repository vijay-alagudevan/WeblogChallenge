package com.paytm.WebLogChallenge

import java.sql.Timestamp

case class AccessLogEntries(
                             timeStamp: String,
                             client: String,
                             request_processing_time: String,
                             backend_processing_time: String,
                             response_processing_time: String,
                             request: String)