#!/usr/bin/env bash
/home/falk/Documents/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
  --class io.qimia.uhrwerk.cli.CommandLineInterface
  --jars <path_to_some_jar>
  --master local[*] \
  /path/to/jar \
  -g "../uhrwerk-example/src/main/resources/testing-env-config.yml" \
  -c "../uhrwerk-example/src/main/resources/testing-connection-config.yml" \
  -t "../uhrwerk-example/src/main/resources/retail_examples/" \
  -r "staging_retail_salesFact_1.0" \
  -st "20200601" \
  -et "20200603"
