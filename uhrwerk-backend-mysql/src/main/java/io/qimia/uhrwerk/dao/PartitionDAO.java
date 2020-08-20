package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.Partition;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class PartitionDAO {
  private static String INSERT =
      "INSERT INTO PARTITION_(id,target_id, partition_ts, year, month, day, hour, minute)  VALUES (?,?,?,?,?,?,?)";
  private final Connection db;

  public PartitionDAO(Connection db) {
    this.db = db;
  }

  public static Partition save(java.sql.Connection db, Partition partition) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    setInsertParams(partition, insert);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    Long id = null;
    if (generatedKeys.next()) id = generatedKeys.getLong(1);
    partition.setId(id);
    return partition;
  }

  private static void setInsertParams(Partition partition, PreparedStatement insert)
      throws SQLException {
    insert.setLong(1, partition.getTargetId());
    insert.setTimestamp(2, Timestamp.valueOf(partition.getPartitionTs()));
    insert.setString(3, partition.getYear());
    insert.setString(4, partition.getMonth());
    insert.setString(5, partition.getDay());
    insert.setString(6, partition.getHour());
    insert.setString(7, partition.getMinute());
  }

  public static Partition[] save(java.sql.Connection db, Partition[] partitions)
      throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (Partition partition : partitions) {
      setInsertParams(partition, insert);
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(partitions.length);
    int i = 0;
    while (generatedKeys.next() && i < partitions.length) {
      partitions[i].setId(generatedKeys.getLong(1));
      i++;
    }
    return partitions;
  }

  public Partition[] getPartitions(Long targetId, LocalDateTime[] partitionTs) {
    return null;
  }
}
