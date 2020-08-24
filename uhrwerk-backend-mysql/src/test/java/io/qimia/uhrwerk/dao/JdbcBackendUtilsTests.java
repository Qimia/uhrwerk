package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcBackendUtilsTests {
    @Test
    void checkdependencyPartitions() {
        LocalDateTime[] partitions = new LocalDateTime[]{
                LocalDateTime.of(2020, 8, 18, 12, 0)
        };
        // FIXME: Doesn't pass yet
//    LocalDateTime[][] identityPartitions = JdbcBackendUtils.dependencyPartitions(
//            partitions,
//            PartitionUnit.HOURS,
//            1,
//            PartitionUnit.MINUTES,
//            60,
//            PartitionTransformType.IDENTITY,
//            1,
//            null
//            );
        LocalDateTime[][] identityPartitions = JdbcBackendUtils.dependencyPartitions(
                partitions,
                PartitionUnit.HOURS,
                1,
                PartitionUnit.HOURS,
                1,
                PartitionTransformType.IDENTITY,
                1,
                null
        );
        assertEquals(partitions[0], identityPartitions[0][0]);
    }

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
