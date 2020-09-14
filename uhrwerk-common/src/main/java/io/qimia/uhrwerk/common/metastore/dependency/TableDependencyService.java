package io.qimia.uhrwerk.common.metastore.dependency;

import io.qimia.uhrwerk.common.model.Table;

import java.sql.SQLException;
import java.time.LocalDateTime;

public interface TableDependencyService {

  /**
   * Find out which partitions have already been processed for the current table, which are ready to be processed
   * and which ones can't be processed yet. This check is being done on a list of given partitions which the caller want
   * to check.
   * @param table a Table which needs to be processed / produced
   * @param partitionTs which partitions need to be processed for this table
   * @return TablePartitionResultSet containing all info required for the processing and failure reporting
   * @throws SQLException
   */
  public TablePartitionResultSet processingPartitions(Table table, LocalDateTime[] partitionTs) throws SQLException;
}
