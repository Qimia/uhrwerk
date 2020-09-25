#!/bin/bash

export SPARK_MASTER_HOST=$(hostname)

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

export SPARK_HOME=/spark

cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
echo "spark.eventLog.enabled true" >>$SPARK_HOME/conf/spark-defaults.conf
echo "spark.eventLog.dir /spark_logs" >>$SPARK_HOME/conf/spark-defaults.conf

cd /spark/bin && /spark/sbin/../bin/spark-class org.apache.spark.deploy.master.Master \
  --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT
