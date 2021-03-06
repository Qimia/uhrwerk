#!/usr/bin/env bash
spark-submit \
  --class io.qimia.uhrwerk.cli.CommandLineInterface \
  --jars /home/falk/IdeaProjects/uhrwerk/uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT.jar \
  --master "local[*]" \
  /home/falk/IdeaProjects/uhrwerk/uhrwerk-cli/target/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  -g "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/testing-env-config.yml" \
  -c "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/testing-connection-config.yml" \
  -t "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/retail_examples/staging/retail/productDimApp_1.0.yml" \
  -t "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/retail_examples/staging/retail/employeeDimApp_1.0.yml" \
  -t "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/retail_examples/staging/retail/storeDimApp_1.0.yml" \
  -t "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/retail_examples/staging/retail/salesFactApp_1.0.yml" \
  -t "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/retail_examples/dwh/retail/salesFactApp_1.0.yml" \
  -r "dwh.retail.salesFact.1.0" \
  -st "2020-06-01T00:00:00" \
  -et "2020-06-03T00:00:00"