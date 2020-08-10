package io.qimia.uhrwerk.backend;

import io.qimia.uhrwerk.config.model.Target;

import java.time.LocalDateTime;
import java.util.Set;

/**
 * Which exact targets need to be written for what partition
 */
public class TargetNeeded {
    LocalDateTime partitionTS;
    Set<Target> targets;

    public TargetNeeded(LocalDateTime partitionTS, Set<Target> targets) {
        this.partitionTS = partitionTS;
        this.targets = targets;
    }

    public LocalDateTime getPartitionTS() {
        return partitionTS;
    }

    public void setPartitionTS(LocalDateTime partitionTS) {
        this.partitionTS = partitionTS;
    }

    public Set<Target> getTargets() {
        return targets;
    }

    public void setTargets(Set<Target> targets) {
        this.targets = targets;
    }
}
