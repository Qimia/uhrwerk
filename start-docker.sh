#!/bin/bash

if [ -z $1 ] || [ $1 == "all" ] || [ $1 == "spark-only" ]; then
  mkdir -p docker/spark_datadir
  mkdir -p docker/spark_logs
  mkdir -p docker/spark_jars
  chmod 777 docker/spark_datadir
  chmod 777 docker/spark_logs
  chmod 777 docker/spark_jars
fi

if [ -z $1 ] || [ $1 == "all" ]; then
  docker-compose -f docker/docker-compose-all.yml up -d
elif [ $1 == "spark-only" ]; then
  docker-compose -f docker/docker-compose-spark-only.yml up -d
else
  docker-compose -f docker/docker-compose-metastore-only.yml up -d
fi

sleep 5
