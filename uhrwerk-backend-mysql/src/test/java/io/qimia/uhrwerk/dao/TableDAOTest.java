package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableDAOTest {

    java.sql.Connection db;
    TableDAO service;

    Source[] generateSources(long tableId) {
        var sources = new Source[2];

        for (int i = 0; i < sources.length; i++) {
            sources[i] = SourceDAOTest.generateSource();
            sources[i].setPath("path" + i);
            sources[i].setTableId(tableId);
            sources[i].setKey();
        }

        return sources;
    }

    private Table generateTable() {
        Table table = new Table();
        table.setArea("dwh");
        table.setVertical("vertical1");
        table.setName("tableA");
        table.setPartitionUnit(PartitionUnit.MINUTES);
        table.setPartitionSize(15);
        table.setParallelism(8);
        table.setMaxBulkSize(96);
        table.setVersion("1.0");
        table.setKey();

        table.setSources(generateSources(table.getId()));
        table.setDependencies(generateDependencies(table.getId()));
        table.setTargets(generateTargets(table.getId()));

        return table;
    }

    Dependency[] generateDependencies(long tableId) {
        var dependencies = new Dependency[2];

        for (int i = 0; i < dependencies.length; i++) {
            Table table = new Table();
            table.setArea("staging");
            table.setVertical("vertical2");
            table.setName("tableDep" + i);
            table.setPartitionUnit(PartitionUnit.MINUTES);
            table.setPartitionSize(15);
            table.setParallelism(8);
            table.setMaxBulkSize(96);
            table.setVersion("1.0");
            table.setKey();
            service.save(table, false);

            var target = generateTarget(table.getId(), i);
            new TargetDAO(db).save(new Target[]{target}, target.getTableId(), false);

            dependencies[i] = new Dependency();
            dependencies[i].setTransformType(PartitionTransformType.IDENTITY);
            dependencies[i].setTransformPartitionUnit(PartitionUnit.MINUTES);
            dependencies[i].setTransformPartitionSize(15);
            dependencies[i].setFormat(target.getFormat());
            dependencies[i].setTableName(table.getName());
            dependencies[i].setArea(table.getArea());
            dependencies[i].setVertical(table.getVertical());
            dependencies[i].setVersion(table.getVersion());
            dependencies[i].setTableId(tableId);
            dependencies[i].setKey();
        }

        return dependencies;
    }

    Target generateTarget(long tableId, int i) {
        var target = new Target();
        var connection = SourceDAOTest.generateConnection();
        new ConnectionDAO(db).save(connection, true);

        target.setFormat("csv" + i);
        target.setTableId(tableId);
        target.setConnection(connection);
        target.setKey();

        return target;
    }

    Target[] generateTargets(long tableId) {
        var targets = new Target[2];

        for (int i = 0; i < targets.length; i++) {
            targets[i] = generateTarget(tableId, i);
            targets[i].setKey();
        }

        return targets;
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws SQLException {
        db =
                DriverManager.getConnection(
                        "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
        service = new TableDAO(db);

        // clean
        db.prepareStatement("delete from TABLE_").execute();
        db.prepareStatement("delete from TARGET").execute();
        db.prepareStatement("delete from SOURCE").execute();
        db.prepareStatement("delete from DEPENDENCY").execute();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws SQLException {
        if (db != null) if (!db.isClosed()) db.close();
    }

    @Test
    void saveTable() {
        var table = SourceDAOTest.generateTable();
        var tableResult = service.save(table, true);

        assertTrue(tableResult.isSuccess());
        assertFalse(tableResult.isError());
    }

    @Test
    void saveTableWithAllChildren() {
        var table = generateTable();

        var result = service.save(table, false);

        assertTrue(result.isSuccess());
        Arrays.stream(result.getSourceResults()).forEach(sourceResult -> assertTrue(sourceResult.isSuccess()));

        assertTrue(result.getTargetResult().isSuccess());

        assertTrue(result.getDependencyResult().isSuccess());
    }

    @Test
    void savingChangedTableTwiceShouldFail() {
        var table = generateTable();

        var result = service.save(table, false);

        assertTrue(result.isSuccess());

        table.setParallelism(9874263);
        result = service.save(table, false);

        assertTrue(result.isError());
    }

    @Test
    void savingSameTableTwiceShouldSucceed() {
        var table = generateTable();

        var result = service.save(table, false);

        assertTrue(result.isSuccess());

        result = service.save(table, false);

        System.out.println(result.getMessage());
        assertTrue(result.isSuccess());
    }

    @Test
    void savingChangedTableTwiceShouldSucceedWithOverwrite() {
        var table = generateTable();

        var result = service.save(table, false);

        assertTrue(result.isSuccess());

        table.setParallelism(9874263);
        result = service.save(table, true);

        assertTrue(result.isSuccess());
    }
}
