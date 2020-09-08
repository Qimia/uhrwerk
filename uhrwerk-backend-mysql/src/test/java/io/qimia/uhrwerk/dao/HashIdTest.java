package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.Dependency;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

public class HashIdTest {

  @Test
  void test() {
    Dependency dep = new Dependency();
    dep.setArea("test-area");
    dep.setVertical("test-vertical");
    dep.setTableName("test-table");
    dep.setFormat("json");
    dep.setVersion("version");
    dep.getArea().toCharArray();
    StringBuilder res =
        new StringBuilder()
            .append(dep.getArea())
            .append(dep.getVertical())
            .append(dep.getTableName())
            .append(dep.getVersion());
    System.out.println("without format: " + res.toString());
    long tableId = LongHashFunction.xx().hashChars(res);
    System.out.println("tableId: " + tableId);
    res = res.append(dep.getFormat());
    System.out.println("with format: " + res.toString());
    long depId = LongHashFunction.xx().hashChars(res);
    System.out.println("depId: " + depId);
  }
}
