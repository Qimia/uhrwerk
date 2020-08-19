package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ConfigBuilderTest {
  @Test
  public void dagBuilderTest() {

    Connection connection_s3 = (new ConnectionBuilder())
            .name("S3")
            .s3_path("Path")
            .s3_secret_id("ID")
            .s3_secret_key("key")
            .build();
    System.out.println(connection_s3);

    Connection connection_jdbc = (new ConnectionBuilder())
            .name("JDBC")
            .jdbc_url("url")
            .jdbc_driver("driver")
            .jdbc_user("user")
            .jdbc_pass("pass")
            .build();
    System.out.println(connection_jdbc);


  }

  @Test
  public void metastoreBuilderTest() {

    Metastore metastore = (new MetastoreBuilder())
            .jdbc_url("url")
            .jdbc_driver("driver")
            .user("user")
            .pass("pass")
            .build();

    System.out.println(metastore);

  }


}