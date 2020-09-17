package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.ConnectionType;
import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.config.representation.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class YamlConfigReader {

  private String readQueryOrFile(String queryOrFile){
    if (queryOrFile.endsWith(".sql")) {
      String query = "FILEQUERY FAILED";
      try {
        query = new String(Files.readAllBytes(Paths.get(Thread.currentThread().getContextClassLoader().getResource(queryOrFile).getPath())));
      } catch (IOException e) {
        e.printStackTrace();
      }
      return query;
    }
    else {return queryOrFile;}
  }

  private String readQueryOrFileLines(String queryOrFile){
    if (queryOrFile.endsWith(".sql")) {
      String query = "FILEQUERY FAILED";
      StringBuilder contentBuilder = new StringBuilder();
      try (Stream<String> stream = Files.lines( Paths.get( Thread.currentThread().getContextClassLoader().getResource(queryOrFile).getPath() ) )){
        stream.forEach(s -> contentBuilder.append(s).append(" "));
      } catch (IOException e) {
        e.printStackTrace();
        return query;
      }
      return contentBuilder.toString();
    }
    else {return queryOrFile;}
  }

  public io.qimia.uhrwerk.common.model.Table getModelTable(Table table){
    if (table != null) {
      table.validate("");
      io.qimia.uhrwerk.common.model.Table result =
              new io.qimia.uhrwerk.common.model.Table();
      io.qimia.uhrwerk.common.model.Table tab =
              new io.qimia.uhrwerk.common.model.Table();
      result = tab;
      tab.setArea(table.getArea());
      tab.setVertical(table.getVertical());
      tab.setName(table.getTable());
      tab.setVersion(table.getVersion());
      tab.setParallelism(table.getParallelism());
      tab.setMaxBulkSize(table.getMax_bulk_size());
      if (table.getPartition() != null) {
        tab.setPartitionUnit(getModelPartitionUnit(table.getPartition().getUnit()));
        tab.setPartitionSize(table.getPartition().getSize());
        tab.setPartitioned(true);
      }
      tab.setKey();
      Source[] sources = table.getSources();
      if (sources != null) {
        io.qimia.uhrwerk.common.model.Source[] resultSource =
                new io.qimia.uhrwerk.common.model.Source[sources.length];
        for (int j = 0; j < sources.length; j++) {
          io.qimia.uhrwerk.common.model.Connection conn =
                  new io.qimia.uhrwerk.common.model.Connection();
          io.qimia.uhrwerk.common.model.Source source =
                  new io.qimia.uhrwerk.common.model.Source();
          resultSource[j] = source;
          source.setTableId(tab.getId());
          conn.setName(sources[j].getConnection_name());
          conn.setKey();
          source.setConnection(conn);
          source.setPath(sources[j].getPath());
          source.setFormat(sources[j].getFormat());
          if (sources[j].getPartition() != null) {
            source.setPartitionUnit(getModelPartitionUnit(sources[j].getPartition().getUnit()));
            source.setPartitionSize(sources[j].getPartition().getSize());
            source.setPartitioned(true);
            source.setSelectQuery(readQueryOrFileLines(sources[j].getSelect().getQuery()));
            source.setSelectColumn(sources[j].getSelect().getColumn());
          } else {
            if (sources[j].getSelect() != null) {
              source.setSelectQuery(readQueryOrFileLines(sources[j].getSelect().getQuery()));
              if (!sources[j].getSelect().getColumn().equals("")) {
                source.setSelectColumn(sources[j].getSelect().getColumn());
              }
            }
          }
          if (sources[j].getParallel_load() != null) {
            source.setParallelLoadQuery(readQueryOrFileLines(sources[j].getParallel_load().getQuery()));
            source.setParallelLoadColumn(sources[j].getParallel_load().getColumn());
            source.setParallelLoadNum(sources[j].getParallel_load().getNum());
          }
          source.setAutoloading(sources[j].getAutoloading());
          source.setKey();
        }
        tab.setSources(resultSource);
      }

      Target[] targets = table.getTargets();
      if (targets != null) {
        io.qimia.uhrwerk.common.model.Target[] resultTarget =
                new io.qimia.uhrwerk.common.model.Target[targets.length];
        for (int j = 0; j < targets.length; j++) {
          io.qimia.uhrwerk.common.model.Connection conn =
                  new io.qimia.uhrwerk.common.model.Connection();
          io.qimia.uhrwerk.common.model.Target target =
                  new io.qimia.uhrwerk.common.model.Target();
          resultTarget[j] = target;
          target.setTableId(tab.getId());
          conn.setName(targets[j].getConnection_name());
          conn.setKey();
          target.setConnection(conn);
          target.setFormat(targets[j].getFormat());
          target.setKey();
        }
        tab.setTargets(resultTarget);
      }

      Dependency[] dependencies = table.getDependencies();
      if (dependencies != null) {
        io.qimia.uhrwerk.common.model.Dependency[] resultDependency =
                new io.qimia.uhrwerk.common.model.Dependency[dependencies.length];
        for (int j = 0; j < dependencies.length; j++) {
          io.qimia.uhrwerk.common.model.Dependency dep =
                  new io.qimia.uhrwerk.common.model.Dependency();
          resultDependency[j] = dep;
          dep.setTableId(tab.getId());
          dep.setArea(dependencies[j].getArea());
          dep.setVertical(dependencies[j].getVertical());
          dep.setTableName(dependencies[j].getTable());
          dep.setFormat(dependencies[j].getFormat());
          dep.setVersion(dependencies[j].getVersion());
          if (dependencies[j].getTransform() != null) {
          switch (dependencies[j].getTransform().getType()) {
            case "identity":
              dep.setTransformType(PartitionTransformType.IDENTITY);
              dep.setTransformPartitionUnit(getModelPartitionUnit("hours"));
              dep.setTransformPartitionSize(1);
              break;
            case "aggregate":
              dep.setTransformType(PartitionTransformType.AGGREGATE);
              dep.setTransformPartitionUnit(getModelPartitionUnit("hours"));
              dep.setTransformPartitionSize(dependencies[j].getTransform().getPartition().getSize());
              break;
            case "window":
              dep.setTransformType(PartitionTransformType.WINDOW);
              dep.setTransformPartitionUnit(getModelPartitionUnit("hours"));
              dep.setTransformPartitionSize(dependencies[j].getTransform().getPartition().getSize());
              break;
            case "temporal_aggregate":
              dep.setTransformType(PartitionTransformType.TEMPORAL_AGGREGATE);
              dep.setTransformPartitionUnit(getModelPartitionUnit(dependencies[j].getTransform().getPartition().getUnit()));
              dep.setTransformPartitionSize(dependencies[j].getTransform().getPartition().getSize());
              break;
            case "none":
              dep.setTransformType(PartitionTransformType.NONE);
              break;
          }
          }
          else {
            dep.setTransformType(PartitionTransformType.NONE);
          }
          dep.setKey();
        }
        tab.setDependencies(resultDependency);
      }

      return result;
    }
    else {return null;}
  }

  public io.qimia.uhrwerk.common.model.Table[] getModelTables(Table[] tables){
    if (tables != null){
      int tablesLength = tables.length;
      io.qimia.uhrwerk.common.model.Table[] resultTables =
              new io.qimia.uhrwerk.common.model.Table[tablesLength];
      for (int j = 0; j < tablesLength; j++) {
        resultTables[j] = getModelTable(tables[j]);
      }
      return resultTables;
    }
    else {return null;}
  }


  public io.qimia.uhrwerk.common.model.Connection getModelConnection(Connection connection){
    connection.validate("");
    io.qimia.uhrwerk.common.model.Connection result =
            new io.qimia.uhrwerk.common.model.Connection();
      io.qimia.uhrwerk.common.model.Connection conn =
              new io.qimia.uhrwerk.common.model.Connection();
      result = conn;
      conn.setName(connection.getName());
      if(connection.getJdbc()!=null){
        conn.setType(ConnectionType.JDBC);
        conn.setJdbcUrl(connection.getJdbc().getJdbc_url());
        conn.setJdbcDriver(connection.getJdbc().getJdbc_driver());
        conn.setJdbcUser(connection.getJdbc().getUser());
        conn.setJdbcPass(connection.getJdbc().getPass());
      }
      if(connection.getS3()!=null){
        conn.setType(ConnectionType.S3);
        conn.setPath(connection.getS3().getPath());
        conn.setAwsAccessKeyID(connection.getS3().getSecret_id());
        conn.setAwsSecretAccessKey(connection.getS3().getSecret_key());
      }
      if(connection.getFile()!=null){
        conn.setType(ConnectionType.FS);
        conn.setPath(connection.getFile().getPath());
      }
      conn.setKey();
    return result;
  }

  public io.qimia.uhrwerk.common.model.Connection[] getModelConnections(Connection[] connections){
    if (connections != null){
      int connectionsLength = connections.length;
      io.qimia.uhrwerk.common.model.Connection[] resultConnections =
              new io.qimia.uhrwerk.common.model.Connection[connectionsLength];
      for (int j = 0; j < connectionsLength; j++) {
        resultConnections[j] = getModelConnection(connections[j]);
      }
      return resultConnections;
    }
    else {return null;}
  }

  public io.qimia.uhrwerk.common.model.Metastore getModelMetastore(Metastore metastore){
    metastore.validate("");
    io.qimia.uhrwerk.common.model.Metastore result =
            new io.qimia.uhrwerk.common.model.Metastore();
    result.setJdbc_url(metastore.getJdbc_url());
    result.setJdbc_driver(metastore.getJdbc_driver());
    result.setUser(metastore.getUser());
    result.setPass(metastore.getPass());
    return result;
  }

  public io.qimia.uhrwerk.common.model.Dag getModelDag(Dag dag){
    dag.validate("");
    io.qimia.uhrwerk.common.model.Dag result =
            new io.qimia.uhrwerk.common.model.Dag();
    result.setConnections(getModelConnections(dag.getConnections()));
    result.setTables(getModelTables(dag.getTables()));
    return result;
  }

  private io.qimia.uhrwerk.common.model.PartitionUnit getModelPartitionUnit(String unit) {
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


  public io.qimia.uhrwerk.common.model.Connection[] readConnections(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Connection[] connections = yaml.loadAs(stream, Connection[].class);
    return getModelConnections(connections);
  }

  public io.qimia.uhrwerk.common.model.Dag readDag(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Dag dag = yaml.loadAs(stream, Dag.class);
    return getModelDag(dag);
  }


  public io.qimia.uhrwerk.common.model.Metastore readEnv(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Metastore metastore = yaml.loadAs(stream, Env.class).getMetastore();
    return (getModelMetastore(metastore));
  }


  public io.qimia.uhrwerk.common.model.Table readTable(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    try {
      Table table = yaml.loadAs(stream, Table.class);
      return getModelTable(table);
    } catch (YAMLException e) {
      throw new IllegalArgumentException("Something went wrong with reading the config file. " +
              "Either it doesn't exist or the structure is wrong?", e);
    }
  }
}

