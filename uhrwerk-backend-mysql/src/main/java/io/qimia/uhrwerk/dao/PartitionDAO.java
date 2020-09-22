package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.PartitionResult;
import io.qimia.uhrwerk.common.metastore.config.PartitionService;
import io.qimia.uhrwerk.common.model.Partition;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.common.tools.TimeTools;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class PartitionDAO implements PartitionService {
  private java.sql.Connection db;

  public PartitionDAO(java.sql.Connection db) {
    this.db = db;
  }

  private static final String INSERT_PARTITION =
          "INSERT INTO PARTITION_(id, target_id, partition_ts, partitioned)\n" + "VALUES(?,?,?,?)";

  private static final String SELECT_BY_ID =
          "SELECT pt.id, pt.target_id, pt.partition_ts, pt.partitioned, tb.partition_unit, tb.partition_size\n"
                  + "FROM PARTITION_ pt\n"
                  + "JOIN TARGET t ON pt.target_id = t.id\n"
                  + "JOIN TABLE_ tb ON t.table_id = tb.id\n"
                  + "WHERE pt.id =?";

  private static final String DELETE_BY_ID = "DELETE FROM PARTITION_ WHERE id = ?";

  private static final String SELECT_BY_TARGET_ID_AND_TS =
          "SELECT pt.id, pt.target_id, pt.partition_ts, pt.partitioned, tb.partition_unit, tb.partition_size\n"
                  + "FROM PARTITION_ pt\n"
                  + "JOIN TARGET t ON pt.target_id = t.id\n"
                  + "JOIN TABLE_ tb ON t.table_id = tb.id\n"
                  + "WHERE pt.target_id = ?\n"
                  + "AND pt.partition_ts IN (%s)";

  private static final String SELECT_LATEST_BY_TARGET_ID =
          "SELECT pt.id, pt.target_id, pt.partition_ts, pt.partitioned, tb.partition_unit, tb.partition_size\n"
                  + "FROM PARTITION_ pt\n"
                  + "JOIN TARGET t ON pt.target_id = t.id\n"
                  + "JOIN TABLE_ tb ON t.table_id = tb.id\n"
                  + "WHERE pt.target_id = ?\n"
                  + "order by pt.partition_ts desc\n"
                  + "limit 1";

  /**
   * Saves the partition to the Metastore, doesn't do any checking anymore.
   *
   * @param partition Partition to save.
   * @throws SQLException         Thrown when something went wrong with the saving. This is processed by the
   *                              caller of this function.
   * @throws NullPointerException Thrown probably only when partition.connection or
   *                              partition.partitionUnit are null. Caught and processed again by the caller of this
   *                              function.
   */
  private void saveToDb(Partition partition) throws SQLException, NullPointerException {
    PreparedStatement insert =
        db.prepareStatement(INSERT_PARTITION, Statement.RETURN_GENERATED_KEYS);

    insert.setLong(1, partition.getId());
    insert.setLong(2, partition.getTargetId());
    insert.setTimestamp(3, Timestamp.valueOf(partition.getPartitionTs()));
    insert.setBoolean(4, partition.isPartitioned());

    JdbcBackendUtils.singleRowUpdate(insert);
  }

  /**
   * Selects a partition from a prepared statement and fills the result into a new Partition object.
   *
   * @param select Prepared select statement.
   * @return Found partition or null otherwise.
   * @throws SQLException When something goes wrong with the SQL command.
   */
  private Partition getPartition(PreparedStatement select) throws SQLException {
    ResultSet record = select.executeQuery();
    if (record.next()) {
      Partition res = new Partition();
      res.setId(record.getLong("pt.id"));
      res.setTargetId(record.getLong("pt.target_id"));
      res.setPartitionTs(record.getTimestamp("pt.partition_ts").toLocalDateTime());
      res.setPartitioned(record.getBoolean("pt.partitioned"));
      res.setPartitionUnit(PartitionUnit.valueOf(record.getString("tb.partition_unit")));
      res.setPartitionSize(record.getInt("tb.partition_size"));

      return res;
    }
    return null;
  }

  /**
   * Find a partition in the Metastore by its id.
   *
   * @param id Id of the partition.
   * @return Found partition or null otherwise.
   * @throws SQLException When something goes wrong with the SQL command.
   */
  public Partition getById(Long id) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);
    return getPartition(select);
  }

  /**
   * Deletes a partition by its id.
   *
   * @param id Partition id
   */
  private void deleteById(Long id) throws SQLException {
    PreparedStatement delete = db.prepareStatement(DELETE_BY_ID);
    delete.setLong(1, id);
    delete.executeUpdate();
  }

  @Override
  public PartitionResult save(Partition partition, boolean overwrite) {
    PartitionResult result = new PartitionResult();
    result.setNewResult(partition);
    try {
      if (!overwrite) {
        Partition oldPartition = getById(partition.getId());

        if (oldPartition != null) {
          result.setOldResult(oldPartition);

          if (!oldPartition.equals(partition)) {
            result.setMessage(
                    String.format(
                            "A Partition with id=%d and different values already exists in the Metastore.",
                            partition.getId()));
            result.setSuccess(false);
          } else {
            result.setSuccess(true);
          }

          return result;
        }
      } else {
        System.out.println("Deleting the old partition");
        deleteById(partition.getId());
      }
      saveToDb(partition);
      result.setSuccess(true);
      result.setOldResult(partition);
    } catch (SQLException | NullPointerException e) {
      result.setError(true);
      result.setSuccess(false);
      result.setException(e);
      result.setMessage(e.getMessage());
    }
    return result;
  }

  @Override
  public PartitionResult[] save(Partition[] partitions, boolean overwrite) {
    PartitionResult[] results = new PartitionResult[partitions.length];

    for (int i = 0; i < partitions.length; i++) {
      results[i] = save(partitions[i], overwrite);
    }

    return results;
  }

  @Override
  public Partition[] getPartitions(Long targetId, LocalDateTime[] partitionTs) {
    PreparedStatement select;
    try {
      String formattedStatement =
              String.format(
                      SELECT_BY_TARGET_ID_AND_TS,
                      Arrays.stream(partitionTs)
                              .map(ts -> "'" + TimeTools.convertTSToUTCString(ts) + "'")
                              .collect(Collectors.joining(",")));
      select = db.prepareStatement(formattedStatement);
      select.setLong(1, targetId);

      ResultSet record = select.executeQuery();
      ArrayList<Partition> partitions = new ArrayList<>();

      while (record.next()) {
        Partition res = new Partition();
        res.setId(record.getLong("pt.id"));
        res.setTargetId(record.getLong("pt.target_id"));
        res.setPartitionTs(record.getTimestamp("pt.partition_ts").toLocalDateTime());
        res.setPartitioned(record.getBoolean("pt.partitioned"));
        var partitionUnit = record.getString("tb.partition_unit");
        if (partitionUnit != null) {
          res.setPartitionUnit(PartitionUnit.valueOf(partitionUnit));
        }
        res.setPartitionSize(record.getInt("tb.partition_size"));

        partitions.add(res);
      }

      return partitions.toArray(new Partition[0]);
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }

    return null;
  }

  @Override
  public Partition getLatestPartition(Long targetId) {
    try {
      PreparedStatement select = db.prepareStatement(SELECT_LATEST_BY_TARGET_ID);
      select.setLong(1, targetId);

      ResultSet record = select.executeQuery();
      Partition partition = null;

      if (record.next()) {
        partition = new Partition();
        partition.setId(record.getLong("pt.id"));
        partition.setTargetId(record.getLong("pt.target_id"));
        partition.setPartitionTs(record.getTimestamp("pt.partition_ts").toLocalDateTime());
        partition.setPartitioned(record.getBoolean("pt.partitioned"));
        var partitionUnit = record.getString("tb.partition_unit");
        if (partitionUnit != null) {
          partition.setPartitionUnit(PartitionUnit.valueOf(partitionUnit));
        }
        partition.setPartitionSize(record.getInt("tb.partition_size"));
      }

      return partition;
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }

    return null;
  }
}
