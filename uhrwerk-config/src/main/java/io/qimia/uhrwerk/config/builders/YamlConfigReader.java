package io.qimia.uhrwerk.config.builders;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel;
import io.qimia.uhrwerk.common.metastore.model.DagModel;
import io.qimia.uhrwerk.common.metastore.model.DependencyModel;
import io.qimia.uhrwerk.common.metastore.model.MetastoreModel;
import io.qimia.uhrwerk.common.metastore.model.SecretModel;
import io.qimia.uhrwerk.common.metastore.model.SourceModel;
import io.qimia.uhrwerk.common.metastore.model.TableModel;
import io.qimia.uhrwerk.common.model.TargetModel;
import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Dag;
import io.qimia.uhrwerk.config.representation.Dependency;
import io.qimia.uhrwerk.config.representation.Env;
import io.qimia.uhrwerk.config.representation.Metastore;
import io.qimia.uhrwerk.config.representation.Secret;
import io.qimia.uhrwerk.config.representation.Source;
import io.qimia.uhrwerk.config.representation.Table;
import io.qimia.uhrwerk.config.representation.Target;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.yaml.snakeyaml.error.YAMLException;

import static io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper;

public class YamlConfigReader {

  public TableModel getModelTable(Table table) {
    if (table != null) {
      table.validate("");
      TableModel tableModel = ModelMapper.toTable(table);

      Source[] sources = table.getSources();
      if (sources != null) {
        SourceModel[] sourceModels = new SourceModel[sources.length];
        for (int j = 0; j < sources.length; j++) {
          ConnectionModel conn = ModelMapper.toConnection(sources[j].getConnectionName());
          sourceModels[j] = ModelMapper.toSource(sources[j], tableModel, conn);
        }
        tableModel.setSources(sourceModels);
      }

      Target[] targets = table.getTargets();
      if (targets != null) {
        TargetModel[] resultTarget = new TargetModel[targets.length];
        for (int j = 0; j < targets.length; j++) {
          ConnectionModel conn = ModelMapper.toConnection(targets[j].getConnectionName());
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

  public SecretModel[] getModelSecrets(Secret[] secrets) {
    if (secrets != null && secrets.length > 0)
      return Arrays.stream(secrets).map(ModelMapper::toSecret).toArray(SecretModel[]::new);
    return null;
  }

  public MetastoreModel getModelMetastore(Metastore metastore) {
    metastore.validate("");
    MetastoreModel result = new MetastoreModel();
    result.setJdbc_url(metastore.getJdbcUrl());
    result.setJdbc_driver(metastore.getJdbcDriver());
    result.setUser(metastore.getUser());
    result.setPass(metastore.getPassword());
    return result;
  }

  public DagModel getModelDag(Dag dag) {
    dag.validate("");
    DagModel result = new DagModel();
    result.setSecrets(getModelSecrets(dag.getSecrets()));
    result.setConnections(getModelConnections(dag.getConnections()));
    result.setTables(getModelTables(dag.getTables()));
    return result;
  }

  public ConnectionModel[] readConnections(String file) {
    InputStream stream = MapperUtils.getInputStream(file);
    Connection[] connections;
    try {
      connections = objectMapper().readValue(stream, Connection[].class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return getModelConnections(connections);
  }

  public SecretModel[] readSecrets(String file) {
    InputStream stream = MapperUtils.getInputStream(file);
    Secret[] secrets;
    try {
      secrets = objectMapper().readValue(stream, Secret[].class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return getModelSecrets(secrets);
  }

  public DagModel readDag(String file) {
    InputStream stream = MapperUtils.getInputStream(file);
    Dag dag = null;
    try {
      dag = objectMapper().readValue(stream, Dag.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return getModelDag(dag);
  }

  public MetastoreModel readEnv(String file) {
    InputStream stream = MapperUtils.getInputStream(file);
    Metastore metastore = null;
    try {
      metastore = objectMapper().readValue(stream, Env.class).getMetastore();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return (getModelMetastore(metastore));
  }

  public TableModel readTable(String file) {
    InputStream stream = MapperUtils.getInputStream(file);
    try {
      Table table = objectMapper().readValue(stream, Table.class);
      return getModelTable(table);
    } catch (YAMLException e) {
      throw new IllegalArgumentException(
          "Something went wrong with reading the config file. "
              + "Either it doesn't exist or the structure is wrong?",
          e);
    } catch (StreamReadException e) {
      throw new RuntimeException(e);
    } catch (DatabindException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
