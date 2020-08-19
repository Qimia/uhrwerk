package io.qimia.uhrwerk;

import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;

import java.time.Duration;
import java.util.ArrayList;

public class PartitionDurationTester {

    /**
     * Convert a PartitionUnit + size pair (denoting a length of time) to a single java Duration object
     * @param u Partition Unit enum
     * @param size amount of partition unit
     * @return a Duration object which can be compared
     */
    public static Duration convertToDuration(PartitionUnit u, int size) {
        Duration partitionSize = Duration.ZERO;
        switch (u) {
            case WEEKS:
                partitionSize = Duration.ofDays(7 * size);
                break;
            case DAYS:
                partitionSize = Duration.ofDays(size);
                break;
            case HOURS:
                partitionSize = Duration.ofHours(size);
                break;
            case MINUTES:
                partitionSize = Duration.ofMinutes(size);
                break;
            default:
                System.err.println("Unsupported partitionunit given");
        }
        return partitionSize;
    }

    /**
     * Result object for checking dependencies
     */
    public static class PartitionTestResult {
        public boolean success;
        public String[] badTableNames;
    }

    /**
     * Input for checking dependency partition sizes
     */
    public static class PartitionTestDependencyInput {
        public String dependencyTableName;
        public PartitionUnit dependencyTablePartitionUnit;
        public int dependencyTablePartitionSize;

        public PartitionTransformType transformType;
        public int transformSize;
    }

    /**
     * Check for a single dependency if the size is correctly set or not
     *
     * @param tablePartitionSize
     * @param testObj
     * @return
     */
    public static boolean checkDependency(
            Duration tablePartitionSize,
            PartitionTestDependencyInput testObj
    ) {
        Duration dependencyTableSize = convertToDuration(
                testObj.dependencyTablePartitionUnit,
                testObj.dependencyTablePartitionSize
        );
        if ((testObj.transformType == PartitionTransformType.IDENTITY) ||
                (testObj.transformType == PartitionTransformType.WINDOW)) {
            return dependencyTableSize.equals(tablePartitionSize);
        } else if (testObj.transformType == PartitionTransformType.AGGREGATE) {
            long minutesDividedOfTable = tablePartitionSize.toMinutes() / testObj.transformSize;
            return dependencyTableSize.equals(Duration.ofMinutes(minutesDividedOfTable));
        }
        return false;
    }

    /**
     *
     * @param tablePartitionUnit
     * @param tablePartitionSize
     * @param dependencies
     * @return
     */
    public static PartitionTestResult checkDependencies(
            PartitionUnit tablePartitionUnit,
            int tablePartitionSize,
            PartitionTestDependencyInput[] dependencies
    ) {
        var result = new PartitionTestResult();
        result.success = true;
        result.badTableNames = new String[0];

        ArrayList<String> badTables = new ArrayList<>();
        Duration tableDuration = convertToDuration(tablePartitionUnit, tablePartitionSize);
        for (PartitionTestDependencyInput testDependency : dependencies) {
            var checkRes = checkDependency(tableDuration, testDependency);
            if (!checkRes) {
                badTables.add(testDependency.dependencyTableName);
            }
        }
        if (!badTables.isEmpty()) {
            result.success = false;
            result.badTableNames = badTables.toArray(new String[0]);
        }
        return result;
    }

}
