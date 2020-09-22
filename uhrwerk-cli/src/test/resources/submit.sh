#!/usr/bin/env bash
/home/falk/Documents/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
  --class io.qimia.uhrwerk.cli.CommandLineInterface
  --jars
  --master local[*] \
  /path/to/jar \
  -g <path_to_global_conf> \
  -c <path_to_connection_conf> \
  -t <path_to_table_conf> \
  -r <run_table> \
  -st <start_time> \
  -et <end_time> \
