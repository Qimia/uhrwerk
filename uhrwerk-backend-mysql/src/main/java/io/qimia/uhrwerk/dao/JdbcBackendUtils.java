package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import org.apache.commons.collections.bidimap.DualHashBidiMap;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class JdbcBackendUtils {
  public static Long singleRowUpdate(PreparedStatement statement) throws SQLException {
    statement.executeUpdate();
    ResultSet generatedKeys = statement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  public static LocalDateTime[][] dependencyPartitions(
      LocalDateTime[] tablePartitions,
      PartitionUnit tableUnit,
      int tableSize,
      PartitionUnit depTableUnit,
      int depTableSize,
      PartitionTransformType transformType,
      int transformSize,
      PartitionUnit transformUnit) {
    if (transformType.equals(PartitionTransformType.IDENTITY)) {
      assert tableUnit.equals(depTableUnit) && tableSize == depTableSize;
      LocalDateTime[][] result = new LocalDateTime[tablePartitions.length][1];
      for (int i = 0; i < tablePartitions.length; i++) {
        result[i][0] = tablePartitions[i];
      }

      return result;
    }
    if (transformType.equals(PartitionTransformType.AGGREGATE)) {
      ChronoUnit tableChronoUnit = ChronoUnit.valueOf(tableUnit.name());
      ChronoUnit depTableChronoUnit = ChronoUnit.valueOf(depTableUnit.name());
      Duration tableDuration = Duration.of(tableSize, tableChronoUnit);
      Duration depDuration = Duration.of(depTableSize, depTableChronoUnit);
      Duration transformDuration = Duration.of(transformSize * depTableSize, depTableChronoUnit);
      assert tableDuration.equals(transformDuration);
      LocalDateTime[][] result = new LocalDateTime[tablePartitions.length][transformSize];
      for (int j = 0; j < tablePartitions.length; j++) {
        for (int i = 0; i < transformSize; i++) {
          result[j][i] = tablePartitions[j].plus(depDuration.multipliedBy(i));
        }
      }
      return result;
    }
    if (transformType.equals(PartitionTransformType.WINDOW)) {
      assert tableUnit.equals(depTableUnit) && tableSize == depTableSize;
      ChronoUnit depTableChronoUnit = ChronoUnit.valueOf(depTableUnit.name());
      Duration depDuration = Duration.of(depTableSize, depTableChronoUnit);
      LocalDateTime[][] result = new LocalDateTime[tablePartitions.length][transformSize];
      for (int j = 0; j < tablePartitions.length; j++) {
        for (int i = 0; i < transformSize; i++) {
          result[j][transformSize - 1 - i] = tablePartitions[j].minus(depDuration.multipliedBy(i));
        }
      }
      return result;
    }

    return null;
  }

  public static LocalDateTime[] getPartitionTs(
      LocalDateTime start, LocalDateTime end, Duration duration) {
    int divided = (int) Duration.between(start, end).dividedBy(duration);
    LocalDateTime[] partitionTs = new LocalDateTime[divided + 1];
    for (int i = 0; i <= divided; i++) {
      partitionTs[i] = start.plus(duration.multipliedBy(i));
    }
    return partitionTs;
  }
}
