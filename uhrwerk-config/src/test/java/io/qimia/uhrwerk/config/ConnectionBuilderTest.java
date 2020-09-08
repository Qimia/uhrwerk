package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class ConnectionBuilderTest {

  @Test
  void builderTest() {
    var builder = new ConnectionBuilder();

    var connection1 = builder
            .name("s3")
            .s3()
              .path("s3Path")
              .secretKey("secretKey")
              .secretId("secretID")
            .done()
            .build();

    var builder2 = new ConnectionBuilder();

    var connection2 = builder2
            .name("file")
            .file()
            .path("filePath")
            .done()
            .build();

    var builder3 = new ConnectionBuilder();

    var connection3 = builder3
            .name("jdbc")
            .jdbc()
            .jdbcUrl("myURL")
            .jdbcDriver("myDriver")
            .user("user")
            .pass("pass")
            .done()
            .build();

    System.out.println(connection1);
    System.out.println(connection2);
    System.out.println(connection3);

  }

  @Test
  void nestedBuilderTest() {
    var builder = new ConnectionBuilder();
    var s3Builder = new S3Builder().path("s3Path")
            .secretKey("secretKey")
            .secretId("secretID");
    var s3 = new S3Builder().path("s3Path2")
            .secretKey("secretKey")
            .secretId("secretID")
            .build();

    var con1 = builder
            .name("s3_1")
            .s3(s3Builder)
            .build();

    var con2 = builder
            .name("s3_2")
            .s3(s3)
            .build();

    System.out.println(con1);
    System.out.println(con2);
  }


}