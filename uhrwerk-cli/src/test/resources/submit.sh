#!/usr/bin/env bash
/home/falk/Documents/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
  --class io.qimia.uhrwerk.cli.CommandLineInterface \
  --jars /home/falk/IdeaProjects/uhrwerk/uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT.jar \
  --master "local[*]" \
  /home/falk/IdeaProjects/uhrwerk/uhrwerk-cli/target/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  -g "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/testing-env-config.yml" \
  -c "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/testing-connection-config.yml" \
  -t "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/retail_examples/staging.retail/productDimApp_1.0.yml" \
  -r "staging.retail.productDimApp.1.0" \
  -st "2020-06-01T00:00:00" \
  -et "2020-06-03T00:00:00"
