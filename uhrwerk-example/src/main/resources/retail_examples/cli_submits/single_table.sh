#!/usr/bin/env bash
../../../../../../cli-tools/uhrwerk-start.py
--uhwerk_config ../../testing-env-config.yml
--table_configs ../staging/retail/productDimApp_1.0.yml
--conn_configs ../../testing-connection-config.yml
--lower_bound 2020-06-01T00:00:00
--upper_bound 2020-06-03T00:00:00
--uhrwerk_jar_location ../../../../../../uhrwerk-cli/target/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar
dwh.retail.productDim.1.0
../../../../../target/uhrwerk-example-0.1.0-SNAPSHOT.jar