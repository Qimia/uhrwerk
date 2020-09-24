#!/bin/bash

mvn package -DskipTests=true
spark-submit --master "local[*]" --driver-memory "2g" --driver-cores 6 --class io.qimia.uhrwerk.example.yelp.LoaderD --jars uhrwerk-cli/target/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT.jar
