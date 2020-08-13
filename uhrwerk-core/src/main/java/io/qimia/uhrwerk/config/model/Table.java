package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.config.PartitionUnit;

public class Table {

  String area;
  String vertical;
  String name;
  String version;
  int parallelism;
  int maxBulkSize;
  PartitionUnit partitionUnit;
  int partitionSize;
  Dependency[] dependencies;
  Source[] sources;
  Target[] targets;
}
