package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

class ParallelLoadBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new ParallelLoadBuilder();

    var parallelLoad = builder
            .column("id")
            .query("SELECT * from TABLE1")
            .num(5)
            .build();
    logger.info(parallelLoad);
  }

}