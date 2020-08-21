package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;

public class JdbcBackendUtilsTests {
  @Test
  public void dependencyPartitionsTests() {
    LocalDateTime[] partitions = new LocalDateTime[2];
    partitions[0] = LocalDateTime.of(2020, 8, 18, 12, 0);
    partitions[1] = LocalDateTime.of(2020, 8, 18, 12, 30);
    LocalDateTime[][] aggPartitions =
        JdbcBackendUtils.dependencyPartitions(
            partitions,
            PartitionUnit.MINUTES,
            30,
            PartitionUnit.MINUTES,
            10,
            PartitionTransformType.AGGREGATE,
            3,
            null);
    for (int i = 0; i < partitions.length; i++) {
      System.out.println("Table-partition: " + partitions[i]);
      System.out.println("Dependency-partition: " + Arrays.toString(aggPartitions[i]));
    }

    LocalDateTime[][] windowPartitions =
        JdbcBackendUtils.dependencyPartitions(
            partitions,
            PartitionUnit.MINUTES,
            30,
            PartitionUnit.MINUTES,
            30,
            PartitionTransformType.WINDOW,
            4,
            null);
    for (int i = 0; i < partitions.length; i++) {
      System.out.println("Table-partition: " + partitions[i]);
      System.out.println("Dependency-partition: " + Arrays.toString(windowPartitions[i]));
    }
  }
}
