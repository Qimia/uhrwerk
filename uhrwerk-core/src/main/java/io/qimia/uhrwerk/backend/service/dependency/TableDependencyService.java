package io.qimia.uhrwerk.backend.service.dependency;

import io.qimia.uhrwerk.config.model.Table;
import java.time.LocalDateTime;

public interface TableDependencyService {

  public TablePartitionResultSet processingPartitions(Table table, LocalDateTime[] partitionTs);
}
