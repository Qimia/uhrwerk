package io.qimia.uhrwerk.config.dao.data;

import io.qimia.uhrwerk.backend.model.PartitionTransform;
import io.qimia.uhrwerk.backend.model.data.Dependency;
import io.qimia.uhrwerk.config.DependencyType;

public class DependencyDAO {
    public static Dependency convertDependencyToBackend(io.qimia.uhrwerk.config.model.Dependency dependency) {
        Dependency backendDependency = new Dependency();
        backendDependency.setBatchSize(dependency.getPartitionCount());
        backendDependency.setBatchTemporalUnit(dependency.getBatchTemporalUnit());
        if (dependency.getTypeEnum() == DependencyType.AGGREGATE) {
            backendDependency.setPartitionTransform(PartitionTransform.AGGREGATE);
        } else if (dependency.getTypeEnum() == DependencyType.WINDOW) {
            backendDependency.setPartitionTransform(PartitionTransform.WINDOW);
        } else if (dependency.getTypeEnum() == DependencyType.ONEONONE) {
            backendDependency.setPartitionTransform(PartitionTransform.ONEONONE);
        }

        return backendDependency;
    }
}
