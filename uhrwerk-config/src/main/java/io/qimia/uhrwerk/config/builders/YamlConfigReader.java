package io.qimia.uhrwerk.config.builders;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.JsonMappingException;
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel;
import io.qimia.uhrwerk.common.metastore.model.ConnectionsModel;
import io.qimia.uhrwerk.common.metastore.model.DagModel;
import io.qimia.uhrwerk.common.metastore.model.DependencyModel;
import io.qimia.uhrwerk.common.metastore.model.MetastoreModel;
import io.qimia.uhrwerk.common.metastore.model.SecretModel;
import io.qimia.uhrwerk.common.metastore.model.SourceModel2;
import io.qimia.uhrwerk.common.metastore.model.TableModel;
import io.qimia.uhrwerk.common.model.TargetModel;
import io.qimia.uhrwerk.config.AWSSecretProvider;
import io.qimia.uhrwerk.config.representation.AWSSecret;
import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Connections;
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
import java.util.Map;
import java.util.stream.Collectors;
import org.yaml.snakeyaml.error.YAMLException;

import static io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper;

public class YamlConfigReader {

  public static final String SECRET = "!secret:";

  public TableModel getModelTable(Table table) {
    if (table != null) {
      table.validate("");
      TableModel tableModel = ModelMapper.toTable(table);

      Source[] sources = table.getSources();
      if (sources != null) {
        SourceModel2[] sourceModels = new SourceModel2[sources.length];
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
    if (secrets != null && secrets.length > 0) {
      return Arrays.stream(secrets).map(ModelMapper::toSecret).toArray(SecretModel[]::new);
    }
    return null;
  }

  public MetastoreModel getModelMetastore(Metastore metastore, Secret[] secrets) {
    metastore.validate("");
    MetastoreModel result = new MetastoreModel();
    result.setJdbc_url(metastore.getJdbcUrl());
    result.setJdbc_driver(metastore.getJdbcDriver());
    String dbUser = metastore.getUser();
    String dbPassword = metastore.getPassword();
    if (secrets != null && secrets.length > 0) {
      Map<String, Secret> secretMap =
          Arrays.stream(secrets).collect(Collectors.toMap(s -> s.getName(), s -> s));
      dbUser = getSecretValue(dbUser, secretMap);
      dbPassword = getSecretValue(dbPassword, secretMap);
    }
    result.setUser(dbUser);
    result.setPass(dbPassword);
    return result;
  }

  private static String getSecretValue(String dbUser, Map<String, Secret> secretMap) {
    if (dbUser != null && !dbUser.isEmpty()) {
      if (dbUser.startsWith(SECRET)) {
        String scrName = dbUser.substring(SECRET.length());
        Secret secret = secretMap.get(scrName);
        if (secret == null) {
          throw new IllegalArgumentException(
              String.format("The secret with name: %s is not specified in config.", scrName));
        }
        AWSSecret awsSecret = (AWSSecret) secret;
        AWSSecretProvider provider = new AWSSecretProvider(awsSecret.getAwsRegion());
        String scrValue = provider.secretValue(awsSecret.getAwsSecretName());
        if (scrValue == null) {
          throw new IllegalArgumentException(String.format("Secret not found in AWS %s ", secret));
        }
        return scrValue;
      }
    }
    return dbUser;
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

  public ConnectionsModel readConnectionsSecrets(String file) {
    InputStream stream = MapperUtils.getInputStream(file);
    Connections connections;
    try {
      connections = objectMapper().readValue(stream, Connections.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new ConnectionsModel(getModelSecrets(
        connections.getSecrets()),
        getModelConnections(connections.getConnections()));
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
    Env env;
    try {
      env = objectMapper().readValue(stream, Env.class);
      return getModelMetastore(env.getMetastore(), env.getSecrets());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    } catch (StreamReadException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
