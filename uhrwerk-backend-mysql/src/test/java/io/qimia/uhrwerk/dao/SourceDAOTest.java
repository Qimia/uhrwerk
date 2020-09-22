package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.ConnectionHelper;
import io.qimia.uhrwerk.common.metastore.config.SourceResult;
import io.qimia.uhrwerk.common.metastore.config.SourceService;
import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public class SourceDAOTest {
    java.sql.Connection db;
    SourceService service;

    static Table generateTable() {
        Table table = new Table();
        table.setArea("test-area-source");
        table.setVertical("test-vertical");
        table.setName("test-table-sourcedao");
        table.setClassName(
            String.join(
                ".", table.getArea(), table.getVertical(), table.getName(), table.getVersion()));
        table.setPartitionUnit(PartitionUnit.MINUTES);
        table.setPartitionSize(15);
        table.setParallelism(8);
        table.setMaxBulkSize(96);
        table.setVersion("1.0");
        table.setPartitioned(true);
        table.setKey();

        return table;
    }

    static Connection generateConnection() {
        Connection connection = new Connection();
        connection.setName("Test-Conn-Source");
        connection.setType(ConnectionType.FS);
        connection.setPath("/some/path/test1");
        connection.setKey();

        return connection;
    }

    static Source generateSource() {
        Table table = generateTable();
        Source source = new Source();
        source.setTableId(table.getId());
        source.setConnection(generateConnection());
        source.setPath("source-test-path");
        source.setFormat("jdbc");
        source.setPartitionUnit(PartitionUnit.DAYS);
        source.setPartitionSize(1);
        source.setParallelLoadNum(40);
        source.setPartitioned(true);
        source.setAutoloading(false);
        source.setKey();
        return source;
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws SQLException {
        db =
                ConnectionHelper.getConnection();
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
    void insert() {
        // first a connection
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.save(table, true);

        Source source = generateSource();

        SourceResult result = service.save(source, table, true);

        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());

        assertEquals(generateTable().getId(), result.getNewResult().getTableId());
        assertEquals(generateConnection(), result.getNewResult().getConnection());
        assertEquals("source-test-path", result.getNewResult().getPath());
        assertEquals("jdbc", result.getNewResult().getFormat());
        assertEquals(PartitionUnit.DAYS, result.getNewResult().getPartitionUnit());
        assertEquals(1, result.getNewResult().getPartitionSize());
        assertEquals(40, result.getNewResult().getParallelLoadNum());
        assertEquals(true, result.getNewResult().isPartitioned());
        assertEquals(false, result.getNewResult().isAutoloading());
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
        tableDAO.save(table, true);

        Source source = generateSource();

        service.save(source, table, false);
        SourceResult resultSame = service.save(source, table, false);

        System.out.println(resultSame.getMessage());
        assertTrue(resultSame.isSuccess());
        assertFalse(resultSame.isError());
        assertNotNull(resultSame.getNewResult());
        assertEquals(source, resultSame.getOldResult());
        assertEquals(source, resultSame.getNewResult());

//        source.setPath("different-path");
        source.setPartitioned(false);
        SourceResult resultDifferent = service.save(source, table, false);
        assertFalse(resultDifferent.isError());
        assertFalse(resultDifferent.isSuccess());
        assertNotNull(resultDifferent.getNewResult());
        assertEquals(source, resultDifferent.getNewResult());
    }

    @Test
    void overwriteOfFieldsShouldBePossible() throws SQLException {
        // first a connection
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.save(table, true);

        Source source = generateSource();

        SourceResult result = service.save(source, table, true);

        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());

        source.setPartitionSize(5);
        source.setPath("new-path");
        source.setSelectColumn("column");
        source.setFormat("parquet");
        source.setPartitionUnit(PartitionUnit.HOURS);
        source.setParallelLoadNum(20);
        source.setPartitioned(false);
        source.setAutoloading(true);

        SourceResult resultChanged = service.save(source, table, true);

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
        tableDAO.save(table, true);

        Source source = generateSource();

        SourceResult result = service.save(source, table, true);

        // without foreign keys this should fail as well
        System.out.println(result.getMessage());
        assertFalse(result.isSuccess());
        assertTrue(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());

        source.setConnection(null);
        SourceResult resultConnectionNull = service.save(source, table, true);

        // with connection null this should fail
        System.out.println(resultConnectionNull.getMessage());
        assertTrue(resultConnectionNull.isError());
        assertFalse(resultConnectionNull.isSuccess());
        assertNotNull(resultConnectionNull.getNewResult());
        assertNotNull(resultConnectionNull.getNewResult().getId());
        assertEquals(source, resultConnectionNull.getNewResult());
    }

    @Test
    void getWithNoConnectionShouldFail() throws SQLException {
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.save(table, true);

        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        Source source = generateSource();

        SourceResult result = service.save(source, table, true);

        System.out.println(result.getMessage());
        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());

        connectionDAO.deleteById(connection.getId());
        assertThrows(NullPointerException.class, () -> service.getSourcesByTableId(table.getId()));
    }

    @Test
    void getWithOnlyConnectionNameShouldFillInTheRest() throws SQLException {
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.save(table, true);

        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        Source source = generateSource();
        Connection onlyName = new Connection();
        onlyName.setName(connection.getName());
        source.setConnection(onlyName);

        SourceResult result = service.save(source, table, true);

        assertTrue(result.isSuccess());
        assertEquals(connection, result.getNewResult().getConnection());

        // works also with id
        Connection onlyId = new Connection();
        onlyId.setId(connection.getId());
        source.setConnection(onlyId);

        SourceResult result2 = service.save(source, table, true);

        assertTrue(result2.isSuccess());
        assertEquals(connection, result2.getNewResult().getConnection());
    }

    @Test
    void insertWithNoTableShouldFail() {
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        Source source = generateSource();

        SourceResult result = service.save(source, null, true);

        // without foreign keys this should work
        System.out.println(result.getMessage());
        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());
        System.out.println(result.getMessage());

        source.setTableId(null);
        SourceResult resultConnectionNull = service.save(source, null, true);

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
        tableDAO.save(table, true);

        Source source = generateSource();

        service.save(source, table, false);

        connection.setName("connection2");
        connection.setKey();
        connectionDAO.save(connection, false);

        source.setConnection(connection);
        // in practice, the source's key should get regenerated and it's then a completely different object

        // updating a source where the connection changed
        SourceResult result = service.save(source, table, true);

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
        tableDAO.save(table, true);

        Source source1 = generateSource();
        Source source2 = generateSource();
        source2.setPath("path2");
        Source source3 = generateSource();
        source3.setPath("path3");
        Source[] sources = {source1, source2, source3};

        SourceResult[] results = service.save(sources, table, true);
        assertEquals(results.length, sources.length);

        for (int i = 0; i < results.length; i++) {
            SourceResult result = results[i];
            assertTrue(result.isSuccess());
            assertFalse(result.isError());
            assertEquals(result.getNewResult(), sources[i]);
        }
    }

    @Test
    void savingWithoutSelectStatementAndPartitionShouldWork() {
        // first a connection
        Connection connection = generateConnection();

        ConnectionDAO connectionDAO = new ConnectionDAO(db);
        connectionDAO.save(connection, true);

        // second a table
        Table table = generateTable();

        TableDAO tableDAO = new TableDAO(db);
        tableDAO.save(table, true);

        Source source = generateSource();

        source.setPartitioned(false);
        source.setPartitionSize(0);
        source.setPartitionUnit(null);
        source.setSelectColumn(null);
        source.setSelectQuery(null);

        SourceResult result = service.save(source, table, true);

        assertTrue(result.isSuccess());
        assertFalse(result.isError());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        assertEquals(source, result.getNewResult());
    }
}
