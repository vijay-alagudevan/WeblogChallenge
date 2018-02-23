#!/usr/bin/env bash

spark_submit_cmd="$SPARK_HOME/bin/spark-submit --master yarn-client"
spark_submit_conf="--conf spark.driver.memory=4g --conf spark.executor.memory=4g --num-executors=4"


className=com.paytm.WebLogChallenge.WebLogMain
params="20150722 \data\*.log.gz 30"

$spark_submit_cmd $spark_submit_conf \
			 --driver-class-path "/home/*.jar" \
			 --jars "WeblogChallenge/target/WeblogChallenge-0.1.jar" \
			 --files="WeblogChallenge/conf/log4j.properties" \
			 --class className params