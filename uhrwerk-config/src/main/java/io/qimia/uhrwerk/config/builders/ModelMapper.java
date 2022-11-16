package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.common.metastore.builders.ConnectionModelBuilder;
import io.qimia.uhrwerk.common.metastore.builders.DependencyModelBuilder;
import io.qimia.uhrwerk.common.metastore.builders.SourceModelBuilder;
import io.qimia.uhrwerk.common.metastore.builders.TableModelBuilder;
import io.qimia.uhrwerk.common.metastore.builders.TargetModelBuilder;
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel;
import io.qimia.uhrwerk.common.metastore.model.ConnectionType;
import io.qimia.uhrwerk.common.metastore.model.DependencyModel;
import io.qimia.uhrwerk.common.metastore.model.PartitionTransformType;
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit;
import io.qimia.uhrwerk.common.metastore.model.SourceModel;
import io.qimia.uhrwerk.common.metastore.model.TableModel;
import io.qimia.uhrwerk.common.model.TargetModel;
import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Dependency;
import io.qimia.uhrwerk.config.representation.File;
import io.qimia.uhrwerk.config.representation.JDBC;
import io.qimia.uhrwerk.config.representation.S3;
import io.qimia.uhrwerk.config.representation.Source;
import io.qimia.uhrwerk.config.representation.Table;
import io.qimia.uhrwerk.config.representation.Target;

public class ModelMapper {
  static TableModel toTable(Table table) {
    TableModelBuilder builder =
        new TableModelBuilder()
            .area(table.getArea())
            .vertical(table.getVertical())
            .name(table.getTable())
            .version(table.getVersion());

    if (table.getClassName() != null) {
      builder.className(table.getClassName());
    } else {
      builder.className(getTableClassName(table));
    }

    builder.parallelism(table.getParallelism()).maxBulkSize(table.getMaxBulkSize());

    if (table.getPartition() != null) {
      builder
          .partitionUnit(toModelPartitionUnit(table.getPartition().getUnit().name().toLowerCase()))
          .partitionSize(table.getPartition().getSize())
          .partitioned(true);
    }
    return builder.build();
  }

  static TableModel toDependencyTable(Dependency dependency) {
    return new TableModelBuilder()
        .area(dependency.getReference().getArea())
        .vertical(dependency.getReference().getVertical())
        .name(dependency.getReference().getTable())
        .version(dependency.getReference().getVersion())
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
    ConnectionModelBuilder builder = new ConnectionModelBuilder().name(connection.getName());
    if (connection instanceof JDBC) {
      var jdbc = (JDBC) connection;
      builder
          .type(ConnectionType.JDBC)
          .jdbcUrl(jdbc.getJdbcUrl())
          .jdbcDriver(jdbc.getJdbcDriver())
          .jdbcUser(jdbc.getUser())
          .jdbcPass(jdbc.getPassword());
    }
    if (connection instanceof S3) {
      var s3 = (S3) connection;
      builder
          .type(ConnectionType.S3)
          .path(s3.getPath())
          .awsAccessKeyID(s3.getSecretId())
          .awsSecretAccessKey(s3.getSecretKey());
    }
    if (connection instanceof File) {
      var file = (File) connection;
      builder.type(ConnectionType.FS).path(file.getPath());
    }
    return builder.build();
  }

  static ConnectionModel toConnection(String connectionName) {
    return new ConnectionModelBuilder().name(connectionName).build();
  }

  static SourceModel toSource(Source source, TableModel table, ConnectionModel conn) {
    SourceModelBuilder sourceBuilder =
        new SourceModelBuilder()
            .table(table)
            .connection(conn)
            .path(source.getPath())
            .format(source.getFormat());

    if (source.getPartition() != null) {
      sourceBuilder
          .partitionUnit(ModelMapper.toModelPartitionUnit(source.getPartition().getUnit().name()))
          .partitionSize(source.getPartition().getSize())
          .partitioned(true)
          .selectQuery(MapperUtils.readQueryOrFileLines(source.getSelect().getQuery()))
          .selectColumn(source.getSelect().getColumn());
    } else {
      if (source.getSelect() != null) {
        sourceBuilder.selectQuery(MapperUtils.readQueryOrFileLines(source.getSelect().getQuery()));
      }
    }
    if (source.getParallelLoad() != null) {
      sourceBuilder
          .parallelLoadQuery(MapperUtils.readQueryOrFileLines(source.getParallelLoad().getQuery()))
          .parallelLoadColumn(source.getParallelLoad().getColumn())
          .parallelLoadNum(source.getParallelLoad().getNum());
    }
    sourceBuilder.autoLoad(source.getAutoLoad());
    return sourceBuilder.build();
  }

  static TargetModel toTarget(Target target, TableModel table, ConnectionModel conn) {
    return new TargetModelBuilder()
        .connection(conn)
        .table(table)
        .format(target.getFormat())
        .build();
  }

  static TargetModel toDependencyTarget(Dependency dependency, TableModel dependencyTable) {
    return new TargetModelBuilder().table(dependencyTable).format(dependency.getFormat()).build();
  }

  static DependencyModel toDependency(Dependency dependency, TableModel table) {

    TableModel dependencyTable = toDependencyTable(dependency);
    TargetModel dependencyTarget = toDependencyTarget(dependency, dependencyTable);

    DependencyModelBuilder builder =
        new DependencyModelBuilder()
            .table(table)
            .area(dependency.getReference().getArea())
            .vertical(dependency.getReference().getVertical())
            .tableName(dependency.getReference().getTable())
            .format(dependency.getFormat())
            .version(dependency.getReference().getVersion())
            .dependencyTable(dependencyTable)
            .dependencyTarget(dependencyTarget);

    if (dependency.getTransform() != null) {
      switch (dependency.getTransform().getType()) {
        case IDENTITY:
          builder
              .transformType(PartitionTransformType.IDENTITY)
              .transformPartitionUnit(toModelPartitionUnit("hours"))
              .transformPartitionSize(1);
          break;
        case AGGREGATE:
          builder
              .transformType(PartitionTransformType.AGGREGATE)
              .transformPartitionUnit(toModelPartitionUnit("hours"))
              .transformPartitionSize(dependency.getTransform().getPartition().getSize());
          break;
        case WINDOW:
          builder
              .transformType(PartitionTransformType.WINDOW)
              .transformPartitionUnit(toModelPartitionUnit("hours"))
              .transformPartitionSize(dependency.getTransform().getPartition().getSize());
          break;
        case NONE:
          builder.transformType(PartitionTransformType.NONE);
          break;
      }
    } else {
      builder.transformType(PartitionTransformType.NONE);
    }
    return builder.build();
  }

  static PartitionUnit toModelPartitionUnit(String unit) {
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
