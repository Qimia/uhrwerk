package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.common.metastore.builders.ConnectionModelBuilder;
import io.qimia.uhrwerk.common.metastore.builders.DependencyModelBuilder;
import io.qimia.uhrwerk.common.metastore.builders.TableModelBuilder;
import io.qimia.uhrwerk.common.metastore.builders.TargetModelBuilder;
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel;
import io.qimia.uhrwerk.common.metastore.model.ConnectionType;
import io.qimia.uhrwerk.common.metastore.model.DependencyModel;
import io.qimia.uhrwerk.common.metastore.model.IngestionMode;
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit;
import io.qimia.uhrwerk.common.metastore.model.SecretModel;
import io.qimia.uhrwerk.common.metastore.model.SecretType;
import io.qimia.uhrwerk.common.metastore.model.SourceModel2;
import io.qimia.uhrwerk.common.metastore.model.TableModel;
import io.qimia.uhrwerk.common.model.TargetModel;
import io.qimia.uhrwerk.config.representation.AWSSecret;
import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Dependency;
import io.qimia.uhrwerk.config.representation.File;
import io.qimia.uhrwerk.config.representation.JDBC;
import io.qimia.uhrwerk.config.representation.Redshift;
import io.qimia.uhrwerk.config.representation.S3;
import io.qimia.uhrwerk.config.representation.Secret;
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

    if (table.getTransformSqlQuery() != null && !table.getTransformSqlQuery().isEmpty()) {
      builder.transformSqlQuery(MapperUtils.readQueryOrFileLines(table.getTransformSqlQuery()));
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
    if (connection instanceof Redshift) {
      var redshift = (Redshift) connection;
      builder.type(ConnectionType.REDSHIFT)
          .jdbcUrl(redshift.getJdbcUrl())
          .jdbcUser(redshift.getUser())
          .jdbcPass(redshift.getPassword())
          .redshiftFormat(redshift.getFormat())
          .redshiftAwsIamRole(redshift.getAwsIamRole())
          .redshiftTempDir(redshift.getTempDir());
    }
    return builder.build();
  }

  static SecretModel toSecret(Secret secret) {
    SecretModel model = new SecretModel();
    if (secret instanceof Secret) {
      AWSSecret aws = (AWSSecret) secret;
      model.setType(SecretType.AWS);
      model.setName(aws.getName());
      model.setAwsSecretName(aws.getAwsSecretName());
      model.setAwsRegion(aws.getAwsRegion());
      return model;
    }
    return null;
  }

  static ConnectionModel toConnection(String connectionName) {
    return new ConnectionModelBuilder().name(connectionName).build();
  }

  static SourceModel2 toSource(Source source, TableModel table, ConnectionModel conn) {
    var model = new SourceModel2();
    model.setTable(table);
    model.setConnection(conn);
    model.setPath(source.getPath());
    model.setFormat(source.getFormat());
    if (source.getPartition() != null) {
      model.setIntervalTempUnit(
          ModelMapper.toModelPartitionUnit(source.getPartition().getUnit().name()));
      model.setIntervalTempSize(source.getPartition().getSize());
      model.setIntervalColumn(source.getPartition().getColumn());
    }
    if (source.getSelect() != null) {
      model.setSelectQuery(MapperUtils.readQueryOrFileLines(source.getSelect().getQuery()));
    }
    if (source.getParallelLoad() != null) {
      model.setParallelPartitionQuery(
          MapperUtils.readQueryOrFileLines(source.getParallelLoad().getQuery()));
      model.setParallelPartitionColumn(source.getParallelLoad().getColumn());
      model.setParallelPartitionNum(source.getParallelLoad().getNum());
    }
    model.setIngestionMode(IngestionMode.valueOf(source.getIngestionMode().name()));
    model.setParallelLoad(source.getAutoLoad());

    return model;
  }

  static TargetModel toTarget(Target target, TableModel table, ConnectionModel conn) {
    return new TargetModelBuilder()
        .connection(conn)
        .table(table)
        .format(target.getFormat())
        .tableName(target.getTableName())
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
            .viewName(dependency.getView())
            .format(dependency.getFormat())
            .version(dependency.getReference().getVersion())
            .dependencyTable(dependencyTable)
            .dependencyTarget(dependencyTarget);
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
      default:
        return null;
    }
  }
}
