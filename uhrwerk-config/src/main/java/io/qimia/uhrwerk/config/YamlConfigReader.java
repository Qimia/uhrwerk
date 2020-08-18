package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.ConnectionType;
import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.config.representation.*;
import org.yaml.snakeyaml.Yaml;
import java.nio.file.Files;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.stream.Stream;

import java.io.InputStream;

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


  private io.qimia.uhrwerk.common.model.Table getModelTable(Table table){
    io.qimia.uhrwerk.common.model.Table result =
            new io.qimia.uhrwerk.common.model.Table();
      table.validate("");
      io.qimia.uhrwerk.common.model.Table tab =
              new io.qimia.uhrwerk.common.model.Table();
      result = tab;
      tab.setArea(table.getArea());
      tab.setVertical(table.getVertical());
      tab.setName(table.getTable());
      tab.setVersion(table.getVersion());
      tab.setParallelism(table.getParallelism());
      tab.setMaxBulkSize(table.getMax_bulk_size());
      tab.setPartitionUnit(getModelPartitionUnit(table.getPartition().getUnit()));
      tab.setPartitionSize(table.getPartition().getSize());
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
          conn.setName(sources[j].getConnection_name());
          source.setConnection(conn);
          source.setPath(sources[j].getPath());
          source.setFormat(sources[j].getFormat());
          source.setPartitionUnit(getModelPartitionUnit(sources[j].getPartition().getUnit()));
          source.setPartitionSize(sources[j].getPartition().getSize());
          source.setParallelLoadQuery(readQueryOrFileLines(sources[j].getParallel_load().getQuery()));
          source.setParallelLoadColumn(sources[j].getParallel_load().getColumn());
          source.setParallelLoadNum(sources[j].getParallel_load().getNum());
          source.setSelectQuery(readQueryOrFileLines(sources[j].getSelect().getQuery()));
          source.setSelectColumn(sources[j].getSelect().getColumn());

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
          dep.setArea(dependencies[j].getArea());
          dep.setVertical(dependencies[j].getVertical());
          dep.setTableName(dependencies[j].getTable());
          dep.setFormat(dependencies[j].getFormat());
          dep.setVersion(dependencies[j].getVersion());
          switch (dependencies[j].getTransform().getType()) {
            case "identity":
              dep.setTransformType(PartitionTransformType.IDENTITY);
              break;
            case "aggregate":
              dep.setTransformType(PartitionTransformType.AGGREGATE);
              dep.setTransformPartitionSize(dependencies[j].getTransform().getPartition().getSize());
              break;
            case "window":
              dep.setTransformType(PartitionTransformType.WINDOW);
              dep.setTransformPartitionSize(dependencies[j].getTransform().getPartition().getSize());
              break;
            case "temporal_aggregate":
              dep.setTransformType(PartitionTransformType.TEMPORAL_AGGREGATE);
              dep.setTransformPartitionUnit(getModelPartitionUnit(dependencies[j].getTransform().getPartition().getUnit()));
              dep.setTransformPartitionSize(dependencies[j].getTransform().getPartition().getSize());
              break;
          }

        }
        tab.setDependencies(resultDependency);
        tab.setKey();
      }
    return result;
  }

  private io.qimia.uhrwerk.common.model.Connection[] getModelConnections(Connection[] connections){
    int connectionLength = connections.length;
    io.qimia.uhrwerk.common.model.Connection[] result =
            new io.qimia.uhrwerk.common.model.Connection[connections.length];
    for (int i = 0; i < connectionLength; i++) {
      io.qimia.uhrwerk.common.model.Connection conn =
              new io.qimia.uhrwerk.common.model.Connection();
      result[i] = conn;
      conn.setName(connections[i].getName());
      if(connections[i].getJdbc()!=null){
        conn.setType(ConnectionType.JDBC);
        conn.setJdbcUrl(connections[i].getJdbc().getJdbc_url());
        conn.setJdbcDriver(connections[i].getJdbc().getJdbc_driver());
        conn.setJdbcUser(connections[i].getJdbc().getUser());
        conn.setJdbcPass(connections[i].getJdbc().getPass());
      }
      if(connections[i].getS3()!=null){
        conn.setType(ConnectionType.S3);
        conn.setPath(connections[i].getS3().getPath());
        conn.setAwsAccessKeyID(connections[i].getS3().getSecret_id());
        conn.setAwsSecretAccessKey(connections[i].getS3().getSecret_key());
      }
      if(connections[i].getFile()!=null){
        conn.setType(ConnectionType.FS);
        conn.setPath(connections[i].getFile().getPath());
      }
      conn.setKey();
    }
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
    io.qimia.uhrwerk.common.model.Connection[] result =
            getModelConnections(connections);
    return result;
  }

  public io.qimia.uhrwerk.common.model.Dag readDag(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Dag dag = yaml.loadAs(stream, Dag.class);
    //dag.validate("");
    io.qimia.uhrwerk.common.model.Dag result =
            new io.qimia.uhrwerk.common.model.Dag();

    result.setConnections(getModelConnections(dag.getConnections()));

    if (dag.getTables() != null) {
      Table[] dagTables = dag.getTables();
      int tableLength = dagTables.length;
    io.qimia.uhrwerk.common.model.Table[] resultTables =
            new io.qimia.uhrwerk.common.model.Table[tableLength];
    for (int j = 0; j < tableLength; j++) {
      resultTables[j] = getModelTable(dagTables[j]);
    }
    result.setTables(resultTables);
  }


    return result;
  }

  public io.qimia.uhrwerk.common.model.Metastore readEnv(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Metastore metastore = yaml.loadAs(stream, Env.class).getMetastore();
    metastore.validate("");
    io.qimia.uhrwerk.common.model.Metastore result =
            new io.qimia.uhrwerk.common.model.Metastore();
      result.setJdbc_url(metastore.getJdbc_url());
      result.setJdbc_driver(metastore.getJdbc_driver());
      result.setUser(metastore.getUser());
      result.setPass(metastore.getPass());
    return result;

  }


  public io.qimia.uhrwerk.common.model.Table readTable(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Table table = yaml.loadAs(stream, Table.class);
    if (table != null) {
      io.qimia.uhrwerk.common.model.Table result =
              getModelTable(table);
      return result;
    }
    else {return null;}
  }


  }

