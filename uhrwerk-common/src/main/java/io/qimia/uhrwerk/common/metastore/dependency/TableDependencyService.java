package io.qimia.uhrwerk.common.metastore.dependency;

import io.qimia.uhrwerk.common.model.Table;

import java.sql.SQLException;
import java.time.LocalDateTime;

public interface TableDependencyService {

  public TablePartitionResultSet processingPartitions(Table table, LocalDateTime[] partitionTs) throws SQLException;
}
