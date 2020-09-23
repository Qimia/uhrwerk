#!/usr/bin/env bash
/home/falk/Documents/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
  --class io.qimia.uhrwerk.cli.CommandLineInterface \
  --jars /home/falk/IdeaProjects/uhrwerk/uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  --master "local[*]" \
  /home/falk/IdeaProjects/uhrwerk/uhrwerk-cli/target/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  -g "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/testing-env-config.yml" \
  -c "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/testing-connection-config.yml" \
  -t "/home/falk/IdeaProjects/uhrwerk/uhrwerk-example/src/main/resources/retail_examples/staging.retail/productDimApp_1.0.yml" \
  -r "staging_retail_productDimApp_1.0" \
  -st "20200601" \
  -et "20200603"
