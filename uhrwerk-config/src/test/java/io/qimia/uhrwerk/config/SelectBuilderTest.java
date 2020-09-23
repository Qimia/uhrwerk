package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

class SelectBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new SelectBuilder();

    var select = builder
            .column("id")
            .query("SELECT * from TABLE1")
            .build();
    logger.info(select);
  }

}