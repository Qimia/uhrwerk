package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.SourceResult;
import io.qimia.uhrwerk.common.metastore.config.SourceService;
import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        // clean up
        db.prepareStatement("delete from SOURCE where id = " + generateSource().getId()).execute();
        db.prepareStatement("delete from CONNECTION where id = " + generateConnection().getId()).execute();
        db.prepareStatement("delete from TABLE_ where id = " + generateTable().getId()).execute();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws SQLException {
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
        System.out.println(result.getMessage());
        if (result.isError()) {
            result.getException().printStackTrace();
        }
        assertTrue(result.isSuccess());
        assertNotNull(result.getNewResult());
        assertNotNull(result.getNewResult().getId());
        System.out.println(result.getNewResult());
    }

//    @Test
//    void update() {
//        Connection conn = new Connection();
//        conn.setName("Test-Conn1");
//        conn.setType(ConnectionType.S3);
//        conn.setPath("/some/path/updated");
//        conn.setAwsAccessKeyID("access-key-id1");
//        conn.setAwsSecretAccessKey("secret-access-key1");
//        ConnectionResult result = service.save(conn, true);
//        assertTrue(result.isSuccess());
//        assertNotNull(result.getNewConnection());
//        assertNotNull(result.getNewConnection().getId());
//        System.out.println(result.getNewConnection());
//    }
}
