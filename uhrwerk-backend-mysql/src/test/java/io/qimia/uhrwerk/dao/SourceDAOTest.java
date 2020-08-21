package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.SourceResult;
import io.qimia.uhrwerk.common.metastore.config.SourceService;
import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public class SourceDAOTest {
    java.sql.Connection db;
    SourceService service;

    private Table generateTable() {
        Table table = new Table();
        table.setArea("test-area-source");
        table.setVertical("test-vertical");
        table.setName("test-table-sourcedao");
        table.setPartitionUnit(PartitionUnit.MINUTES);
        table.setPartitionSize(15);
        table.setParallelism(8);
        table.setMaxBulkSize(96);
        table.setVersion("1.0");
        table.setKey();

        return table;
    }

    Connection generateConnection() {
        Connection connection = new Connection();
        connection.setName("Test-Conn-Source");
        connection.setType(ConnectionType.FS);
        connection.setPath("/some/path/test1");
        connection.setKey();

        return connection;
    }

    Source generateSource() {
        Table table = generateTable();
        Source source = new Source();
        source.setTableId(table.getId());
        source.setConnection(generateConnection());
        source.setPath("source-test-path");
        source.setFormat("jdbc");
        source.setPartitionUnit(PartitionUnit.DAYS);
        source.setPartitionSize(1);
        source.setParallelLoadNum(40);
        source.setKey();
        return source;
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws SQLException {
        db =
                DriverManager.getConnection(
                        "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
        service = new SourceDAO(db);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws SQLException {
        // clean up
        db.prepareStatement("delete from SOURCE where id = " + generateSource().getId()).execute();
        db.prepareStatement("delete from CONNECTION where id = " + generateConnection().getId()).execute();
        db.prepareStatement("delete from TABLE_ where id = " + generateTable().getId()).execute();

        if (db != null) if (!db.isClosed()) db.close();
    }

    @Test
    void insert() throws SQLException {
        // first a connection
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.saveTable(table);

        Source source = generateSource();

        SourceResult result = service.save(source, true);

        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());
    }

    @Test
    void returnFailWhenAlreadyExistingAndDifferent() throws SQLException {
        // first a connection
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.saveTable(table);

        Source source = generateSource();

        service.save(source, false);
        SourceResult resultSame = service.save(source, false);

        assertTrue(resultSame.isSuccess());
        assertFalse(resultSame.isError());
        assertNotNull(resultSame.getNewResult());
        assertEquals(source, resultSame.getOldResult());
        assertEquals(source, resultSame.getNewResult());

        source.setPath("different-path");
        SourceResult resultDifferent = service.save(source, false);
        assertTrue(resultDifferent.isError());
        assertFalse(resultDifferent.isSuccess());
        assertNotNull(resultDifferent.getNewResult());
        assertEquals(source, resultDifferent.getNewResult());
    }

    @Test
    void overwriteOfNonEssentialFieldsShouldBePossible() throws SQLException {
        // first a connection
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.saveTable(table);

        Source source = generateSource();

        SourceResult result = service.save(source, true);

        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());

        source.setPartitionSize(5);
        source.setPath("new-path");
        source.setSelectColumn("column");

        SourceResult resultChanged = service.save(source, true);

        assertTrue(resultChanged.isSuccess());
        assertFalse(resultChanged.isError());
        assertNotNull(resultChanged.getNewResult());
        assertNotNull(resultChanged.getNewResult().getId());
        assertEquals(source, resultChanged.getNewResult());
    }

    @Test
    void insertWithNoConnectionShouldFail() throws SQLException {
        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.saveTable(table);

        Source source = generateSource();

        SourceResult result = service.save(source, true);

        // without foreign keys this should work
        System.out.println(result.getMessage());
        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());

        source.setConnection(null);
        SourceResult resultConnectionNull = service.save(source, true);

        // with connection null this should fail
        System.out.println(resultConnectionNull.getMessage());
        assertTrue(resultConnectionNull.isError());
        assertFalse(resultConnectionNull.isSuccess());
        assertNotNull(resultConnectionNull.getNewResult());
        assertNotNull(resultConnectionNull.getNewResult().getId());
        assertEquals(source, resultConnectionNull.getNewResult());
    }

    @Test
    void insertWithNoTableShouldFail() {
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        Source source = generateSource();

        SourceResult result = service.save(source, true);

        // without foreign keys this should work
        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());
        System.out.println(result.getMessage());

        source.setTableId(null);
        SourceResult resultConnectionNull = service.save(source, true);

        // with table id = null this should fail
        assertTrue(resultConnectionNull.isError());
        assertFalse(resultConnectionNull.isSuccess());
        assertNotNull(resultConnectionNull.getNewResult());
        assertNotNull(resultConnectionNull.getNewResult().getId());
        assertEquals(source, resultConnectionNull.getNewResult());
        System.out.println(resultConnectionNull.getMessage());
    }

    @Test
    void upsertWithADifferentConnectionShouldWork() throws SQLException {
        // first a connection
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.saveTable(table);

        Source source = generateSource();

        service.save(source, false);

        connection.setName("connection2");
        connection.setKey();
        connectionDAO.save(connection, false);

        source.setConnection(connection);
        // in practice, the source's key should get regenerated and it's then a completely different object

        // updating a source where the connection changed
        SourceResult result = service.save(source, true);

        System.out.println(result.getMessage());
        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());
    }

    @Test
    void savingSeveralSourcesAtOnceShouldWork() throws SQLException {
        // first a connection
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.saveTable(table);

        Source source1 = generateSource();
        Source source2 = generateSource();
        source2.setPath("path2");
        Source source3 = generateSource();
        source3.setPath("path3");
        Source[] sources = {source1, source2, source3};

        SourceResult[] results = service.save(sources, true);
        assertEquals(results.length, sources.length);

        for (int i = 0; i < results.length; i++) {
            SourceResult result = results[i];
            assertTrue(result.isSuccess());
            assertFalse(result.isError());
            assertEquals(result.getNewResult(), sources[i]);
        }
    }
}
