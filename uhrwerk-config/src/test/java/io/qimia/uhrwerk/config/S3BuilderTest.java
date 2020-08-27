package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class S3BuilderTest {

  @Test
  void builderTest() {
    var builder = new S3Builder();

    var s3 = builder
            .path("myPath")
            .secretId("secretID")
            .secretKey("secretKey")
            .build();
    System.out.println(s3);
  }

}