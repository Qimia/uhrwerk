package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.ConnectionHelper;
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;
import io.qimia.uhrwerk.common.model.Partition;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionDependencyDAOTest {

    Connection db;

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws SQLException {
        db = ConnectionHelper.getConnection();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws SQLException {
        var deletePartDependencies = db.createStatement();  // In case of some lost source data
        deletePartDependencies.execute("DELETE FROM PARTITION_DEPENDENCY");
        deletePartDependencies.close();
        var deletePartitions = db.createStatement();
        deletePartitions.execute("DELETE FROM PARTITION_");
        deletePartitions.close();
        if (db != null) if (!db.isClosed()) db.close();
    }

    Partition[] setupPartitions(LocalDateTime ts) throws SQLException {
        String insertQuery = "INSERT INTO PARTITION_(id, target_id, partition_ts)\n"
                + "VALUES(?,?,?)";

        // WARNING: uses fake target ids
        long[] fakeIds = {123L, 234L, 345L};
        Partition[] out = new Partition[fakeIds.length];
        for (int i = 0; i < fakeIds.length; i++) {
            var p = new Partition();
            p.setTargetId(fakeIds[i]);
            p.setPartitionTs(ts);
            p.setPartitionUnit(PartitionUnit.MINUTES);
            p.setPartitionSize(20);
            p.setKey();

            PreparedStatement insert = db.prepareStatement(insertQuery);

            insert.setLong(1, p.getId());
            insert.setLong(2, p.getTargetId());
            insert.setTimestamp(3, Timestamp.valueOf(p.getPartitionTs()));
            insert.executeUpdate();
            insert.close();

            out[i] = p;
        }
        return out;
    }

    @Test
    void testSaveGetDelete() throws SQLException {
        LocalDateTime batchTime = LocalDateTime.of(2020, 6, 1, 10, 0);
        var partitions = setupPartitions(batchTime);
        var depRes = new DependencyResult();
        depRes.setPartitionTs(batchTime);
        depRes.setPartitions(new Partition[]{partitions[0], partitions[1]});
        // TODO: Times set for these dependency partitions (and target id's) currently do not make much sense
        // but these are -not- checked when storing dependency partitions

        var dao = new PartitionDependencyDAO(db);
        var insertRes = dao.save(partitions[2].getId(), depRes, true);
        assertTrue(insertRes.isSuccess());
        assertFalse(insertRes.isError());

        insertRes = dao.save(partitions[2].getId(), depRes, false);
        assertFalse(insertRes.isSuccess());
        assertTrue(insertRes.isError());  // unable to overwrite will cause an insert error

        var allChildren = dao.getChildIds(partitions[0].getId());
        assertEquals(partitions[2].getId(), allChildren.get(0));

        var allParents = dao.getParentIds(partitions[2].getId());
        HashSet<Long> parentIds = new HashSet<>();
        parentIds.add(partitions[0].getId());
        parentIds.add(partitions[1].getId());
        for (Long l : allParents) {
            assertTrue(parentIds.contains(l));
        }

        var deleteRes = dao.remove(partitions[2].getId(), depRes);
        assertTrue(deleteRes.isSuccess());
        assertFalse(deleteRes.isError());

        allChildren = dao.getChildIds(partitions[0].getId());
        assertEquals(0, allChildren.size());
        allParents = dao.getParentIds(partitions[2].getId());
        assertEquals(0, allParents.size());
    }

    @Test
    void testMultiple() throws SQLException {
        LocalDateTime batchTime = LocalDateTime.of(2020, 6, 1, 10, 0);
        var partitions = setupPartitions(batchTime);
        var depResA = new DependencyResult();
        depResA.setPartitionTs(batchTime);
        depResA.setPartitions(new Partition[]{partitions[0]});
        var depResB = new DependencyResult();
        depResB.setPartitionTs(batchTime);
        depResB.setPartitions(new Partition[]{partitions[1]});

        var dao = new PartitionDependencyDAO(db);
        var insertRes = dao.saveAll(partitions[2].getId(),
                new DependencyResult[]{depResA, depResB}, false);
        assertTrue(insertRes.isSuccess());
        assertFalse(insertRes.isError());

        insertRes = dao.saveAll(partitions[2].getId(),
                new DependencyResult[]{depResA, depResB}, false);
        assertFalse(insertRes.isSuccess());
        assertTrue(insertRes.isError());

        var deleteRes = dao.removeAll(partitions[2].getId());

        insertRes = dao.saveAll(partitions[2].getId(),
                new DependencyResult[]{depResA, depResB}, false);
        assertTrue(insertRes.isSuccess());
        assertFalse(insertRes.isError());
    }

    @Test
    void gettingPartitions() throws SQLException {
        LocalDateTime batchTime = LocalDateTime.of(2020, 6, 1, 10, 0);
        var partitions = setupPartitions(batchTime);
        var depResA = new DependencyResult();
        depResA.setPartitionTs(batchTime);
        depResA.setPartitions(new Partition[]{partitions[0]});
        var dao = new PartitionDependencyDAO(db);
        var insertRes = dao.save(partitions[2].getId(), depResA, false);
        assertTrue(insertRes.isSuccess());
        assertFalse(insertRes.isError());

        var depResB = new DependencyResult();
        depResB.setPartitionTs(batchTime);
        depResB.setPartitions(new Partition[]{partitions[1]});
        var insertRes2 = dao.save(partitions[2].getId(), depResB, false);
        assertTrue(insertRes2.isSuccess());
        assertFalse(insertRes2.isError());

        var parentPartitions = dao.getParentPartitions(partitions[2]);
        assertEquals(2, parentPartitions.size());
    }

}
