package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.Dependency;
import net.openhft.hashing.LongHashFunction;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

public class HashIdTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void test() {
    Dependency dep = new Dependency();
    dep.setArea("test-area");
    dep.setVertical("test-vertical");
    dep.setTableName("test-table");
    dep.setFormat("json");
    dep.setVersion("version");
    StringBuilder res =
        new StringBuilder()
            .append(dep.getArea())
            .append(dep.getVertical())
            .append(dep.getTableName())
            .append(dep.getVersion());
    logger.info("without format: " + res.toString());
    long tableId = LongHashFunction.xx().hashChars(res);
    logger.info("tableId: " + tableId);
    res = res.append(dep.getFormat());
    logger.info("with format: " + res.toString());
    long depId = LongHashFunction.xx().hashChars(res);
    logger.info("depId: " + depId);
  }
}
