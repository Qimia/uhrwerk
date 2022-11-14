package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.ConnectionModel;
import io.qimia.uhrwerk.common.model.DagModel;
import io.qimia.uhrwerk.common.model.DependencyModel;
import io.qimia.uhrwerk.common.model.MetastoreModel;
import io.qimia.uhrwerk.common.model.SourceModel;
import io.qimia.uhrwerk.common.model.TableModel;
import io.qimia.uhrwerk.common.model.TargetModel;
import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Dag;
import io.qimia.uhrwerk.config.representation.Dependency;
import io.qimia.uhrwerk.config.representation.Env;
import io.qimia.uhrwerk.config.representation.Metastore;
import io.qimia.uhrwerk.config.representation.Source;
import io.qimia.uhrwerk.config.representation.Table;
import io.qimia.uhrwerk.config.representation.Target;
import java.io.InputStream;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

public class YamlConfigReader {

  public TableModel getModelTable(Table table) {
    if (table != null) {
      table.validate("");
      TableModel tableModel = ModelMapper.toTable(table);

      Source[] sources = table.getSources();
      if (sources != null) {
        SourceModel[] sourceModels = new SourceModel[sources.length];
        for (int j = 0; j < sources.length; j++) {
          ConnectionModel conn = ModelMapper.toConnection(sources[j].getConnection_name());
          sourceModels[j] = ModelMapper.toSource(sources[j], tableModel, conn);
        }
        tableModel.setSources(sourceModels);
      }

      Target[] targets = table.getTargets();
      if (targets != null) {
        TargetModel[] resultTarget = new TargetModel[targets.length];
        for (int j = 0; j < targets.length; j++) {
          ConnectionModel conn = ModelMapper.toConnection(targets[j].getConnection_name());
          resultTarget[j] = ModelMapper.toTarget(targets[j], tableModel, conn);
        }
        tableModel.setTargets(resultTarget);
      }

      Dependency[] dependencies = table.getDependencies();
      if (dependencies != null) {
        DependencyModel[] resultDependency = new DependencyModel[dependencies.length];
        for (int j = 0; j < dependencies.length; j++) {
          resultDependency[j] = ModelMapper.toDependency(dependencies[j], tableModel);
        }
        tableModel.setDependencies(resultDependency);
      }
      return tableModel;
    } else {
      return null;
    }
  }

  public TableModel[] getModelTables(Table[] tables) {
    if (tables != null) {
      int tablesLength = tables.length;
      TableModel[] resultTables = new TableModel[tablesLength];
      for (int j = 0; j < tablesLength; j++) {
        resultTables[j] = getModelTable(tables[j]);
      }
      return resultTables;
    } else {
      return null;
    }
  }

  public ConnectionModel[] getModelConnections(Connection[] connections) {
    if (connections != null) {
      int connectionsLength = connections.length;
      ConnectionModel[] resultConnections = new ConnectionModel[connectionsLength];
      for (int j = 0; j < connectionsLength; j++) {
        resultConnections[j] = ModelMapper.toConnectionFull(connections[j]);
      }
      return resultConnections;
    } else {
      return null;
    }
  }

  public MetastoreModel getModelMetastore(Metastore metastore) {
    metastore.validate("");
    MetastoreModel result = new MetastoreModel();
    result.setJdbc_url(metastore.getJdbc_url());
    result.setJdbc_driver(metastore.getJdbc_driver());
    result.setUser(metastore.getUser());
    result.setPass(metastore.getPass());
    return result;
  }

  public DagModel getModelDag(Dag dag) {
    dag.validate("");
    DagModel result = new DagModel();
    result.setConnections(getModelConnections(dag.getConnections()));
    result.setTables(getModelTables(dag.getTables()));
    return result;
  }

  public ConnectionModel[] readConnections(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = MapperUtils.getInputStream(file);
    Connection[] connections = yaml.loadAs(stream, Connection[].class);
    return getModelConnections(connections);
  }

  public DagModel readDag(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = MapperUtils.getInputStream(file);
    Dag dag = yaml.loadAs(stream, Dag.class);
    return getModelDag(dag);
  }

  public MetastoreModel readEnv(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = MapperUtils.getInputStream(file);
    Metastore metastore = yaml.loadAs(stream, Env.class).getMetastore();
    return (getModelMetastore(metastore));
  }

  public TableModel readTable(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = MapperUtils.getInputStream(file);
    try {
      Table table = yaml.loadAs(stream, Table.class);
      return getModelTable(table);
    } catch (YAMLException e) {
      throw new IllegalArgumentException(
          "Something went wrong with reading the config file. "
              + "Either it doesn't exist or the structure is wrong?",
          e);
    }
  }
}
