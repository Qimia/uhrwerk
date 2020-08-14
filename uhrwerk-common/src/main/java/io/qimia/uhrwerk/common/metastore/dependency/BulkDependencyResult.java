package io.qimia.uhrwerk.common.metastore.dependency;

import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.Dependency;
import io.qimia.uhrwerk.common.model.Partition;

import java.time.LocalDateTime;

public class BulkDependencyResult {
    LocalDateTime[] partitionTimestamps;
    Dependency dependency;
    Connection connection;
    Partition[] succeeded;

    public LocalDateTime[] getPartitionTimestamps() {
        return partitionTimestamps;
    }

    public void setPartitionTimestamps(LocalDateTime[] partitionTimestamps) {
        this.partitionTimestamps = partitionTimestamps;
    }

    public Dependency getDependency() {
        return dependency;
    }

    public void setDependency(Dependency dependency) {
        this.dependency = dependency;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Partition[] getSucceeded() {
        return succeeded;
    }

    public void setSucceeded(Partition[] succeeded) {
        this.succeeded = succeeded;
    }
}
