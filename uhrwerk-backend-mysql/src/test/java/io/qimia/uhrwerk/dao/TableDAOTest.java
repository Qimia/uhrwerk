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

    Source[] generateSources() {
        var sources = new Source[2];

        for (int i = 0; i < sources.length; i++) {
            sources[i] = SourceDAOTest.generateSource();
            sources[i].setPath("path" + i);
            sources[i].setKey();
        }

        return sources;
    }

    Dependency generateDependency() {
        var dependency = new Dependency();

        dependency.setArea("a");
        dependency.setDependencyTableId(123L);
        dependency.setDependencyTargetId(456L);
        dependency.setFormat("csv");
        dependency.setTableId(SourceDAOTest.generateTable().getId());
        dependency.setTableName("test-table-sourcedao");
        dependency.setTransformPartitionSize(1);
        dependency.setTransformPartitionUnit(PartitionUnit.DAYS);
        dependency.setTransformType(PartitionTransformType.IDENTITY);
        dependency.setVersion("1");
        dependency.setVertical("jjjjj");
        dependency.setKey();

        return dependency;
    }

    Dependency[] generateDependencies() {
        var dependencies = new Dependency[2];

        for (int i = 0; i < dependencies.length; i++) {
            dependencies[i] = generateDependency();
            var table = SourceDAOTest.generateTable();
            table.setName("dependency" + i);
            table.setKey();
            service.save(table, false);
            dependencies[i].setDependencyTableId(table.getId());
            dependencies[i].setKey();
        }

        return dependencies;
    }

    Target generateTarget() {
        var target = new Target();
        var connection = SourceDAOTest.generateConnection();
        new ConnectionDAO(db).save(connection, true);

        target.setFormat("csv");
        target.setTableId(SourceDAOTest.generateTable().getId());
        target.setConnection(connection);
        target.setKey();

        return target;
    }

    Target[] generateTargets() {
        var targets = new Target[2];

        for (int i = 0; i < targets.length; i++) {
            targets[i] = generateTarget();
            targets[i].setFormat("format" + i);
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
        var table = SourceDAOTest.generateTable();
        table.setSources(generateSources());
        table.setDependencies(generateDependencies());
        table.setTargets(generateTargets());

        var result = service.save(table, false);

        System.out.println(result.getMessage());
        assertTrue(result.isSuccess());
        Arrays.stream(result.getSourceResults()).forEach(sourceResult -> assertTrue(sourceResult.isSuccess()));

        System.out.println(result.getTargetResult().getMessage());
        assertTrue(result.getTargetResult().isSuccess());

        System.out.println(result.getDependencyResult().getMessage());
        assertTrue(result.getDependencyResult().isSuccess());
    }


}
