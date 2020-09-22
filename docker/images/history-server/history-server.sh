#!/bin/bash

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

mkdir -p $SPARK_HISTORY_SERVER_LOG

export SPARK_HOME=/spark

ln -sf /dev/stdout $SPARK_HISTORY_SERVER_LOG/spark-history-server.out

cd /spark/bin && /spark/sbin/../bin/spark-class org.apache.spark.deploy.history.HistoryServer --properties-file /spark.properties >>$SPARK_HISTORY_SERVER_LOG/spark-history-server.out
