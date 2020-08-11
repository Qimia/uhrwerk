package io.qimia.uhrwerk.config.dao.config;

import io.qimia.uhrwerk.config.dao.data.DependencyDAO;
import io.qimia.uhrwerk.config.dao.data.SourceDAO;
import io.qimia.uhrwerk.config.dao.data.TargetDAO;
import io.qimia.uhrwerk.config.model.Table;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class TableDAO {
    public static Long save(java.sql.Connection db, Table table) throws SQLException {
        io.qimia.uhrwerk.backend.model.config.Table backendTable = new io.qimia.uhrwerk.backend.model.config.Table();
        backendTable.setArea(table.getArea());
        backendTable.setVertical(table.getVertical());
        backendTable.setTableName(table.getName());
        backendTable.setBatchTemporalUnit(table.getBatchTemporalUnit());
        backendTable.setBatchSize(table.getBatchSizeAsInt());
        backendTable.setParallelism(table.getParallelism());
        backendTable.setMaxPartitions(table.getMaxBatches());
        backendTable.setVersion(String.valueOf(table.getVersion()));

        if (table.getDependencies() != null) {
            backendTable.setDependencies(Arrays.stream(table.getDependencies()).map(DependencyDAO::convertDependencyToBackend).collect(Collectors.toList()));
            io.qimia.uhrwerk.backend.dao.data.DependencyDAO.save(db, backendTable.getDependencies());
        }
        if (table.getSources() != null) {
            backendTable.setSources(Arrays.stream(table.getSources()).map(SourceDAO::convertSourceToBackend).collect(Collectors.toList()));
            io.qimia.uhrwerk.backend.dao.data.SourceDAO.save(db, backendTable.getSources());
        }
        if (table.getTargets() != null) {
            backendTable.setTargets(Arrays.stream(table.getTargets()).map(TargetDAO::convertTargetToBackend).collect(Collectors.toList()));
            io.qimia.uhrwerk.backend.dao.data.TargetDAO.save(db, backendTable.getTargets());
        }

        return io.qimia.uhrwerk.backend.dao.config.TableDAO.save(db, backendTable);
    }
}
