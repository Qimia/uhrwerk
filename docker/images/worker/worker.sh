#!/bin/bash

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

export SPARK_HOME=/spark

cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
echo "spark.eventLog.enabled true" >>$SPARK_HOME/conf/spark-defaults.conf
echo "spark.eventLog.dir /spark_logs" >>$SPARK_HOME/conf/spark-defaults.conf

/spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER
