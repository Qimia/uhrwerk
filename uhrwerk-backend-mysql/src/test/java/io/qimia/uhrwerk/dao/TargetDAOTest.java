package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.Target;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TargetDAOTest {



    @Test
    void compareTest() {
        var connA = new Connection();
        connA.setName("connA");
        connA.setKey();
        var tarA = new Target();
        tarA.setFormat("jdbc");
        tarA.setTableId(123L);
        tarA.setConnection(connA);
        tarA.setKey();

        var connAFull = new Connection();
        connAFull.setName("connA");
        connAFull.setJdbcDriver("somedriver");
        connAFull.setJdbcUrl("someurl");
        connAFull.setJdbcUser("root");
        connAFull.setJdbcPass("somePass");
        var tarB = new Target();
        tarB.setFormat("jdbc");
        tarB.setTableId(123L);
        tarB.setConnection(connAFull);
        assertTrue(TargetDAO.compareTargets(tarB, tarA));

        tarA.setFormat("parquet");
        tarA.setKey();
        assertFalse(TargetDAO.compareTargets(tarB, tarA));

        tarA.setFormat("jdbc");
        connA.setName("newname");
        connA.setKey();
        tarA.setKey();
        assertFalse(TargetDAO.compareTargets(tarB, tarA));
    }
}
