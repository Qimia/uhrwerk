#!/bin/bash

mvn package -DskipTests=true
cp uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT-jar-with-dependencies.jar docker/spark_datadir/uhrwerk-example-0.1.0-SNAPSHOT-jar-with-dependencies.jar
docker exec -it spark-master /spark/bin/spark-submit --deploy-mode "client" --master "spark://0.0.0.0:7077" --driver-memory "1g" --driver-cores 2 --num-executors 1 --executor-memory "2g" --executor-cores 6 --class io.qimia.uhrwerk.example.yelp.LoaderAParq /spark_datadir/uhrwerk-example-0.1.0-SNAPSHOT-jar-with-dependencies.jar
