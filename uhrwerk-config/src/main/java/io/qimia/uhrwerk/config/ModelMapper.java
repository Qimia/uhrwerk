package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.ConnectionModel;
import io.qimia.uhrwerk.common.model.ConnectionModelBuilder;
import io.qimia.uhrwerk.common.model.ConnectionType;
import io.qimia.uhrwerk.common.model.DependencyModel;
import io.qimia.uhrwerk.common.model.DependencyModelBuilder;
import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.common.model.SourceModel;
import io.qimia.uhrwerk.common.model.SourceModelBuilder;
import io.qimia.uhrwerk.common.model.TableModel;
import io.qimia.uhrwerk.common.model.TableModelBuilder;
import io.qimia.uhrwerk.common.model.TargetModel;
import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Dependency;
import io.qimia.uhrwerk.config.representation.Source;
import io.qimia.uhrwerk.config.representation.Table;
import io.qimia.uhrwerk.config.representation.Target;

public class ModelMapper {
  static TableModel toTable(Table table) {
    TableModelBuilder builder =
        TableModel.builder()
            .area(table.getArea())
            .vertical(table.getVertical())
            .name(table.getTable())
            .version(table.getVersion());

    if (table.getClass_name() != null) {
      builder.className(table.getClass_name());
    } else {
      builder.className(getTableClassName(table));
    }

    builder.parallelism(table.getParallelism()).maxBulkSize(table.getMax_bulk_size());

    if (table.getPartition() != null) {
      builder
          .partitionUnit(toModelPartitionUnit(table.getPartition().getUnit()))
          .partitionSize(table.getPartition().getSize())
          .partitioned(true);
    }
    return builder.build();
  }

  static TableModel toDependencyTable(Dependency dependency) {
    return TableModel.builder()
        .area(dependency.getArea())
        .vertical(dependency.getVertical())
        .name(dependency.getTable())
        .version(dependency.getVersion())
        .build();
  }

  private static String getTableClassName(Table table) {
    return table.getArea()
        + "."
        + table.getVertical()
        + "."
        + table.getTable()
        + "."
        + table.getVersion();
  }

  static ConnectionModel toConnectionFull(Connection connection) {
    connection.validate("");
    ConnectionModelBuilder builder = ConnectionModel.builder().name(connection.getName());
    if (connection.getJdbc() != null) {
      builder
          .type(ConnectionType.JDBC)
          .jdbcUrl(connection.getJdbc().getJdbc_url())
          .jdbcDriver(connection.getJdbc().getJdbc_driver())
          .jdbcUser(connection.getJdbc().getUser())
          .jdbcPass(connection.getJdbc().getPass());
    }
    if (connection.getS3() != null) {
      builder
          .type(ConnectionType.S3)
          .path(connection.getS3().getPath())
          .awsAccessKeyID(connection.getS3().getSecret_id())
          .awsSecretAccessKey(connection.getS3().getSecret_key());
    }
    if (connection.getFile() != null) {
      builder.type(ConnectionType.FS).path(connection.getFile().getPath());
    }
    return builder.build();
  }

  static ConnectionModel toConnection(String connectionName) {
    return ConnectionModel.builder().name(connectionName).build();
  }

  static SourceModel toSource(Source source, TableModel table, ConnectionModel conn) {
    SourceModelBuilder sourceBuilder =
        SourceModel.builder()
            .table(table)
            .connection(conn)
            .path(source.getPath())
            .format(source.getFormat());

    if (source.getPartition() != null) {
      sourceBuilder
          .partitionUnit(ModelMapper.toModelPartitionUnit(source.getPartition().getUnit()))
          .partitionSize(source.getPartition().getSize())
          .partitioned(true)
          .selectQuery(MapperUtils.readQueryOrFileLines(source.getSelect().getQuery()))
          .selectColumn(source.getSelect().getColumn());
    } else {
      if (source.getSelect() != null) {
        sourceBuilder.selectQuery(MapperUtils.readQueryOrFileLines(source.getSelect().getQuery()));
      }
    }
    if (source.getParallel_load() != null) {
      sourceBuilder
          .parallelLoadQuery(MapperUtils.readQueryOrFileLines(source.getParallel_load().getQuery()))
          .parallelLoadColumn(source.getParallel_load().getColumn())
          .parallelLoadNum(source.getParallel_load().getNum());
    }
    sourceBuilder.autoLoad(source.getAutoloading());
    return sourceBuilder.build();
  }

  static TargetModel toTarget(Target target, TableModel table, ConnectionModel conn) {
    return TargetModel.builder().connection(conn).table(table).format(target.getFormat()).build();
  }

  static TargetModel toDependencyTarget(Dependency dependency, TableModel dependencyTable) {
    return TargetModel.builder().table(dependencyTable).format(dependency.getFormat()).build();
  }

  static DependencyModel toDependency(Dependency dependency, TableModel table) {

    TableModel dependencyTable = toDependencyTable(dependency);
    TargetModel dependencyTarget = toDependencyTarget(dependency, dependencyTable);

    DependencyModelBuilder builder =
        DependencyModel.builder()
            .table(table)
            .area(dependency.getArea())
            .vertical(dependency.getVertical())
            .tableName(dependency.getTable())
            .format(dependency.getFormat())
            .version(dependency.getVersion())
            .dependencyTable(dependencyTable)
            .dependencyTarget(dependencyTarget);
    if (dependency.getTransform() != null) {
      switch (dependency.getTransform().getType()) {
        case "identity":
          builder
              .transformType(PartitionTransformType.IDENTITY)
              .transformPartitionUnit(toModelPartitionUnit("hours"))
              .transformPartitionSize(1);
          break;
        case "aggregate":
          builder
              .transformType(PartitionTransformType.AGGREGATE)
              .transformPartitionUnit(toModelPartitionUnit("hours"))
              .transformPartitionSize(dependency.getTransform().getPartition().getSize());
          break;
        case "window":
          builder
              .transformType(PartitionTransformType.WINDOW)
              .transformPartitionUnit(toModelPartitionUnit("hours"))
              .transformPartitionSize(dependency.getTransform().getPartition().getSize());
          break;
        case "none":
          builder.transformType(PartitionTransformType.NONE);
          break;
      }
    } else {
      builder.transformType(PartitionTransformType.NONE);
    }
    return builder.build();
  }

  static io.qimia.uhrwerk.common.model.PartitionUnit toModelPartitionUnit(String unit) {
    switch (unit) {
      case "minute":
      case "minutes":
        return PartitionUnit.MINUTES;
      case "hour":
      case "hours":
        return PartitionUnit.HOURS;
      case "day":
      case "days":
        return PartitionUnit.DAYS;
      case "week":
      case "weeks":
        return PartitionUnit.WEEKS;
      default:
        return null;
    }
  }
}
