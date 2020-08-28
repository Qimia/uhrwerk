package io.qimia.uhrwerk;

import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;

import java.time.Duration;
import java.util.ArrayList;

public class PartitionDurationTester {

  /**
   * Convert a PartitionUnit + size pair (denoting a length of time) to a single java Duration
   * object
   *
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
   * Result object for checking dependencies If a test fails, it will show which tables are the
   * cause of the test failing
   */
  public static class PartitionTestResult {
    public boolean success;
    public String[] badTableNames;
  }

  /**
   * Input for checking dependency partition sizes contains info about the dependency's
   * transformation + info about the partition size of the table its dependending on
   */
  public static class PartitionTestDependencyInput {
    public String dependencyTableName;
    public PartitionUnit dependencyTablePartitionUnit;
    public int dependencyTablePartitionSize;

    public PartitionTransformType transformType;
    public int transformSize;
  }

  /**
   * Check for a single partitioned dependency if the size is correctly set or not
   *
   * @param tablePartitionSize partition size of the table's partition
   * @param testObj Dependency's partitioning information
   * @return true if they match up, false if not
   */
  public static boolean checkPartitionedDependency(
      Duration tablePartitionSize, PartitionTestDependencyInput testObj) {
    Duration dependencyTableSize =
        convertToDuration(
            testObj.dependencyTablePartitionUnit, testObj.dependencyTablePartitionSize);
    if ((testObj.transformType == PartitionTransformType.IDENTITY)
        || (testObj.transformType == PartitionTransformType.WINDOW)) {
      return dependencyTableSize.equals(tablePartitionSize);
    } else if (testObj.transformType == PartitionTransformType.AGGREGATE) {
      long minutesDividedOfTable = tablePartitionSize.toMinutes() / testObj.transformSize;
      return dependencyTableSize.equals(Duration.ofMinutes(minutesDividedOfTable));
    }
    return false;
  }

  /**
   * Check for an array of Dependencies (with included Table partition information) if they are
   * correct for a given target table's partition size
   *
   * @param tablePartitionUnit Unit of time for denoting the target table's partition size
   * @param tablePartitionSize Count denoting how many PartitionUnit makes up the target table's
   *     partition size
   * @param dependencies All the dependencies' transitions + the partition information from their
   *     tables
   * @return result showing if it was correct and if not names the tables which are not agreeing
   *     with the table (target)
   */
  public static PartitionTestResult checkDependencies(
      PartitionUnit tablePartitionUnit,
      int tablePartitionSize,
      PartitionTestDependencyInput[] dependencies) {
    var result = new PartitionTestResult();
    result.success = true;
    result.badTableNames = new String[0];

    ArrayList<String> badTables = new ArrayList<>();
    Duration tableDuration = convertToDuration(tablePartitionUnit, tablePartitionSize);
    for (PartitionTestDependencyInput testDependency : dependencies) {
      if (testDependency.transformType != null) {
        var checkRes = checkPartitionedDependency(tableDuration, testDependency);
        if (!checkRes) {
          badTables.add(testDependency.dependencyTableName);
        }
      }
      // if it is an unpartitioned dependency then existing is enough
      // (and any partitioned table can be loaded as an unpartitioned one)
    }
    if (!badTables.isEmpty()) {
      result.success = false;
      result.badTableNames = badTables.toArray(new String[0]);
    }
    return result;
  }
}
