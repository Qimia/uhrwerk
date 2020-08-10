package io.qimia.uhrwerk.backend;

import io.qimia.uhrwerk.config.model.Dependency;

import java.time.LocalDateTime;
import java.util.Set;

/**
 * Which dependencies were missing for what particular partition of the data
 */
public class DependencyFailed {
    LocalDateTime partitionTS;
    Set<Dependency> missingDependencies;

    public DependencyFailed(LocalDateTime partitionTS, Set<Dependency> missingDependencies) {
        this.partitionTS = partitionTS;
        this.missingDependencies = missingDependencies;
    }

    public LocalDateTime getPartitionTS() {
        return partitionTS;
    }

    public void setPartitionTS(LocalDateTime partitionTS) {
        this.partitionTS = partitionTS;
    }

    public Set<Dependency> getMissingDependencies() {
        return missingDependencies;
    }

    public void setMissingDependencies(Set<Dependency> missingDependencies) {
        this.missingDependencies = missingDependencies;
    }
}
