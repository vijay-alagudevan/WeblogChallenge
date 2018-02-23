# Solution for Web Log Challenge

## Tools/Framework used:
  1. Apache Spark
  2. Scala
  3. Hive
  4. Scala Test
  5. Shell Scripts
  
## Approach:
  1. Read the log file and create a RDD
  2. Convert the RDD into Dataframe using case class by picking certain fields from the log.
  3. Append additional columns to DataFrame necessary for sessionizing the logs
  4. Invoke different methods to get each results.
  
## Execution Flow:
  1. The shell script under /scripts/weblog_runner.sh is the entry point for the code execution
  2. The shell script contains input command line arguments like file location, run date and session time.
  3. The shell script can be called via workflow manager like oozie/airflow.
  4. The shell script calls the main object com.paytm.WebLogChallenge.WebLogMain
  5. The Main object initializes the spark context and calls the MAIN method in WebLog class.
  6. The WebLog class has the business logic and is extended by Helper trait for utility methods.
  7. It also extends the AppBase trait which has the spark initialization.
  8. AccessLogEntries is the case class for the fields in the web log.
  
## Unit Tests:
 1. The unit tests are under src/test/scala. There are 3 unit tests.
 2. The unit tests resources folder has the sample input data and also expected output data for the different methods.
 3. A dataframe is created based on the sample input data. This sample dataframe is passed to the method that need to be tested. The resulted 
  dataframe is matched against the already saved expected output result.
 
