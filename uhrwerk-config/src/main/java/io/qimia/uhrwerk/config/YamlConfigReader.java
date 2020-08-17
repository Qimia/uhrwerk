package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.ConnectionType;
import io.qimia.uhrwerk.config.representation.*;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

public class YamlConfigReader {
  public io.qimia.uhrwerk.common.model.Connection[] readConnections(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Connection[] connections = yaml.loadAs(stream, Connection[].class);
    io.qimia.uhrwerk.common.model.Connection[] result =
        new io.qimia.uhrwerk.common.model.Connection[connections.length];
    for (int i = 0; i < connections.length; i++) {
      //connections[i].validate("");
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

  public io.qimia.uhrwerk.common.model.Dag readDag(String file) {
      Yaml yaml = new Yaml();
      InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
      Dag dag = yaml.loadAs(stream, Dag.class);
      Connection[] connections = dag.getConnections();
      dag.validate("");
      io.qimia.uhrwerk.common.model.Dag result =
              new io.qimia.uhrwerk.common.model.Dag();
      if(connections!=null){
        io.qimia.uhrwerk.common.model.Connection[] conn =
                new io.qimia.uhrwerk.common.model.Connection[connections.length];
        for (int i = 0; i < connections.length; i++){
          conn(i).setName(connections[i].getName());
          if(connections[i].getJdbc()!=null){
            conn(i).setType(ConnectionType.JDBC);
            conn(i).setJdbcUrl(connections[i].getJdbc().getJdbc_url());
            conn(i).setJdbcDriver(connections[i].getJdbc().getJdbc_driver());
            conn(i).setJdbcUser(connections[i].getJdbc().getUser());
            conn(i).setJdbcPass(connections[i].getJdbc().getPass());
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
      }


    return result;
  }

  public io.qimia.uhrwerk.common.model.Metastore readEnv(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Metastore metastore = yaml.loadAs(stream, Uhrwerk.class).getMetastore();
    //metastore.validate("");
    io.qimia.uhrwerk.common.model.Metastore result =
            new io.qimia.uhrwerk.common.model.Metastore();
      result.setJdbc_url(metastore.getJdbc_url());
      result.setJdbc_driver(metastore.getJdbc_driver());
      result.setUser(metastore.getUser());
      result.setPass(metastore.getPass());
    return result;

  }

  public Table readTable(String file) {
    return null;
  }
}
