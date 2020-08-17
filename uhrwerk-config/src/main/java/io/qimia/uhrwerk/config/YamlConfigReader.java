package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.ConnectionType;
import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.config.representation.*;
import org.yaml.snakeyaml.Yaml;

import javax.servlet.http.Part;
import java.io.InputStream;

public class YamlConfigReader {

  private io.qimia.uhrwerk.common.model.Table[] getModelTables(Table[] tables){
    int tablesLength = tables.length
    io.qimia.uhrwerk.common.model.Table[] result =
            new io.qimia.uhrwerk.common.model.Table[tablesLength];
    for (int i = 0; i < tablesLength; i++) {
      //tables[i].validate("");
      io.qimia.uhrwerk.common.model.Table table =
              new io.qimia.uhrwerk.common.model.Table();
      result[i] = table;
      table.setArea(tables[i].getArea());
      table.setVertical(tables[i].getVertical());
      table.setVersion(tables[i].getVersion());
      table.setParallelism(tables[i].getParallelism());
      table.setMaxBulkSize(tables[i].getMax_bulk_size());
      table.setPartitionUnit(getModelPartitionUnit(tables[i].getPartition().getUnit()));
      table.setPartitionSize(tables[i].getPartition().getSize());
      Source[] sources = tables[i].getSources();
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
        source.setParallelLoadQuery(sources[j].getParallel_load().getQuery());
        //TODO: Partition query file
        source.setParallelLoadColumn(sources[j].getParallel_load().getColumn());
        source.setParallelLoadNum(sources[j].getParallel_load().getNum());
        source.setSelectQuery(sources[j].getSelect().getQuery());
        //TODO: select query file
        source.setSelectColumn(sources[j].getSelect().getColumn());

      }
      table.setSources(resultSource);

      Target[] targets = tables[i].getTargets();
      io.qimia.uhrwerk.common.model.Target[] resultTarget =
              new io.qimia.uhrwerk.common.model.Target[targets.length];
      for (int j = 0; j < targets.length; j++) {
        io.qimia.uhrwerk.common.model.Connection conn =
                new io.qimia.uhrwerk.common.model.Connection();
        io.qimia.uhrwerk.common.model.Target target =
                new io.qimia.uhrwerk.common.model.Target();
        resultTarget[j] = target;
        conn.setName(targets[j].getConnection_name());
        target.setConnection(conn);
        target.setFormat(targets[j].getFormat());
      }
      table.setTargets(resultTarget);

      Dependency[] dependencies = tables[i].getDependencies();
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
        switch (dependencies[j].getTransform().getType()){
          case "identity":
            dep.setTransformType(PartitionTransformType.IDENTITY);
            break;
          case "aggregate":
            dep.setTransformType(PartitionTransformType.AGGREGATE);
            break;
          case "window":
            dep.setTransformType(PartitionTransformType.WINDOW);
            break;
          case "temporal_aggregate":
            dep.setTransformType(PartitionTransformType.AGGREGATE);
            //TODO: define Transform Type for temporal_aggregate
        }
        dep.setTransformPartitionUnit(getModelPartitionUnit(dependencies[j].getTransform().getPartition().getUnit()));
        dep.setTransformPartitionSize(dependencies[j].getTransform().getPartition().getSize());

      }

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
        conn.setAwsAccessKeyID(connections[i].getS3().getSecret_id());
        conn.setAwsSecretAccessKey(connections[i].getS3().getSecret_key());
      }
      if(connections[i].getFile()!=null){
        conn.setType(ConnectionType.FS);
        conn.setPath(connections[i].getFile().getPath());
      }
    }
    return result;
  }

  private io.qimia.uhrwerk.common.model.PartitionUnit getModelPartitionUnit(String unit) {
    switch (unit) {
      case "minute":
        return PartitionUnit.MINUTES;
      break;
      case "hour":
        return PartitionUnit.HOURS;
      break;
      case "day":
        return PartitionUnit.DAYS;
      break;
      case "week":
        return PartitionUnit.WEEKS;
      break;
      default:
        return PartitionUnit.HOURS;
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
      result.setTables(getModelTables(dag.getTables()));


    return result;
  }

  public io.qimia.uhrwerk.common.model.Metastore readEnv(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Metastore metastore = yaml.loadAs(stream, Env.class).getMetastore();
    //metastore.validate("");
    io.qimia.uhrwerk.common.model.Metastore result =
            new io.qimia.uhrwerk.common.model.Metastore();
      result.setJdbc_url(metastore.getJdbc_url());
      result.setJdbc_driver(metastore.getJdbc_driver());
      result.setUser(metastore.getUser());
      result.setPass(metastore.getPass());
    return result;

  }

  public io.qimia.uhrwerk.common.model.Table[] readTable(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Table[] tables = yaml.loadAs(stream, Table[].class);
    io.qimia.uhrwerk.common.model.Table[] result =
            getModelTables(tables);
    return result;
    }


  }

