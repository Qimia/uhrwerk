#!/bin/bash

mvn package -DskipTests=true
spark-submit --master "local[*]" --driver-memory "2g" --driver-cores 6 --class io.qimia.uhrwerk.example.yelp.LoaderD --jars uhrwerk-cli/target/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar --driver-java-options "-Dlog4j.configuration=file:uhrwerk-common/src/main/resources/log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:uhrwerk-common/src/main/resources/log4j.properties" uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT.jar
