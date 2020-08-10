package io.qimia.uhrwerk.backend;

import io.qimia.uhrwerk.config.model.Dependency;

import java.time.LocalDateTime;
import java.util.Set;

/**
 * A Java-esque Either Union class
 */
public class DependencyResult {
    private boolean success;
    // Success means there is only a datetime that is ready to run
    private LocalDateTime right = null;
    // Failure means there are some dependencies missing and we need to know what
    private DependencyFailed left = null;

    public DependencyResult(LocalDateTime right) {
        success = true;
        this.right = right;
    }

    public DependencyResult(DependencyFailed left) {
        success = false;
        this.left = left;
    }

    /**
     * Quick constructor for failed dependency checks
     */
    public DependencyResult(LocalDateTime leftPartitionTS, Set<Dependency> leftDepedencies) {
        success = false;
        this.left = new DependencyFailed(leftPartitionTS, leftDepedencies);
    }

    public boolean isSuccess() {
        return success;
    }

    public LocalDateTime getRight() {
        return right;
    }

    public DependencyFailed getLeft() {
        return left;
    }
}
