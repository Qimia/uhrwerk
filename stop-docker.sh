#!/bin/bash

if [ -z $1 ] || [ $1 == "all" ]; then
  docker compose -f docker/docker-compose-all.yml down
elif [ $1 == "spark-only" ]; then
  docker compose -f docker/docker-compose-spark-only.yml down
else
  docker compose -f docker/docker-compose-metastore-only.yml down
fi
