package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.common.metastore.builders.ConnectionModelBuilder;
import io.qimia.uhrwerk.common.metastore.builders.TableModelBuilder;
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
import io.qimia.uhrwerk.config.representation.PartitionMapping;
import io.qimia.uhrwerk.config.representation.Redshift;
import io.qimia.uhrwerk.config.representation.S3;
import io.qimia.uhrwerk.config.representation.Secret;
import io.qimia.uhrwerk.config.representation.Source;
import io.qimia.uhrwerk.config.representation.Table;
import io.qimia.uhrwerk.config.representation.Target;
import io.qimia.uhrwerk.common.utils.TemplateUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

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

    SortedSet<String> templateArgs = new TreeSet<>();
    if (table.getTransformSqlQuery() != null && !table.getTransformSqlQuery().isEmpty()) {
      String transformSqlQuery = MapperUtils.readQueryOrFileLines(table.getTransformSqlQuery());
      builder.transformSqlQuery(transformSqlQuery);
      templateArgs.addAll(TemplateUtils.templateArgs(transformSqlQuery));
    }

    builder.partitionColumns(table.getPartitionColumns());

    SortedSet<String> addVars = new TreeSet<>();
    if (table.getTableVariables() != null
        && table.getTableVariables().length > 0) {
      for (String tableVariable : table.getTableVariables()) {
        boolean inArgs = templateArgs.stream()
            .anyMatch(arg -> arg.equalsIgnoreCase(tableVariable));
        if (!inArgs) {
          addVars.add(tableVariable);
        }
      }
    }
    templateArgs.addAll(addVars);

    if (!templateArgs.isEmpty()) {
      builder.tableVariables(templateArgs.toArray(new String[templateArgs.size()]));
    }

    builder.parallelism(table.getParallelism())
        .maxBulkSize(table.getMaxBulkSize());

    if (table.getPartition() != null) {
      builder
          .partitionUnit(toModelPartitionUnit(table.getPartition().getUnit().name().toLowerCase()))
          .partitionSize(table.getPartition().getSize())
          .partitioned(true);
    }
    return builder.build();
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

  static SourceModel2 toSource(Source source, ConnectionModel conn) {
    var model = new SourceModel2();
    model.setConnection(conn);
    model.setPath(source.getPath());
    model.setFormat(source.getFormat());
    if (source.getPartition() != null) {
      model.setIntervalTempUnit(
          ModelMapper.toModelPartitionUnit(source.getPartition().getUnit().name()));
      model.setIntervalTempSize(source.getPartition().getSize());
      model.setIntervalColumn(source.getPartition().getColumn());
    }
    SortedSet<String> templateArgs = new TreeSet<>();
    if (source.getSelect() != null) {
      var selectQuery = MapperUtils.readQueryOrFileLines(source.getSelect().getQuery());
      model.setSelectQuery(selectQuery);
      templateArgs.addAll(TemplateUtils.templateArgs(selectQuery));
    }

    if (source.getParallelLoad() != null) {
      var parallelQuery = MapperUtils.readQueryOrFileLines(source.getParallelLoad().getQuery());
      templateArgs.addAll(TemplateUtils.templateArgs(parallelQuery));
      model.setParallelPartitionQuery(parallelQuery);
      model.setParallelPartitionColumn(source.getParallelLoad().getColumn());
      model.setParallelPartitionNum(source.getParallelLoad().getNum());
      model.setParallelLoad(true);
    }

    if (!templateArgs.isEmpty()) {
      model.setSourceVariables(templateArgs.toArray(new String[templateArgs.size()]));
    }

    model.setIngestionMode(IngestionMode.valueOf(source.getIngestionMode().name()));
    model.setAutoLoad(source.getAutoLoad());
    model.setViewName(source.getView());
    return model;
  }

  static TargetModel toTarget(Target target, ConnectionModel conn) {

    TargetModel targetModel = new TargetModel();
    targetModel.setConnection(conn);
    targetModel.setFormat(target.getFormat());
    targetModel.setTableName(target.getTableName());
    return targetModel;
  }

  static DependencyModel toDependency(Dependency dependency) {

    List<String> dependencyVariables = new ArrayList<>();

    HashMap<String, String> partitionMappings = new HashMap<>();
    if (dependency.getPartitionMappings() != null && dependency.getPartitionMappings().length > 0) {
      for (PartitionMapping mapping : dependency.getPartitionMappings()
      ) {
        partitionMappings.put(mapping.getColumn(), mapping.getValue());
        if (mapping.getValue().startsWith("$") && mapping.getValue().endsWith("$")) {
          dependencyVariables.add(mapping.getValue().substring(1, mapping.getValue().length() - 1));
        }
      }
    }

    DependencyModel dependencyModel = new DependencyModel();
    dependencyModel.setArea(dependency.getReference().getArea());
    dependencyModel.setVertical(dependency.getReference().getVertical());
    dependencyModel.setTableName(dependency.getReference().getTable());
    dependencyModel.setVersion(dependency.getReference().getVersion());
    dependencyModel.setViewName(dependency.getView());
    dependencyModel.setFormat(dependency.getFormat());
    if (!partitionMappings.isEmpty()) {
      dependencyModel.setPartitionMappings(partitionMappings);
    }

    if (!dependencyVariables.isEmpty()) {
      dependencyModel.setDependencyVariables(
          dependencyVariables.toArray(new String[dependencyVariables.size()]));
    }
    dependencyModel.setAutoLoad(dependency.getAutoLoad());
    return dependencyModel;
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
