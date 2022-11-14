package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParallelLoadBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  void builderTest() {
    var builder = new ParallelLoadBuilder();

    var parallelLoad = builder
            .column("id")
            .query("SELECT * from TABLE1")
            .num(5)
            .build();
    logger.info(parallelLoad.toString());

    assertEquals(5, parallelLoad.getNum());
  }

}