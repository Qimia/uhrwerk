package io.qimia.uhrwerk.backend.service.dependency;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;

public class TablePartitionResult {
  LocalDateTime partitionTs;
  boolean processed;
  boolean resolved;
  DependencyResult[] resolvedDependencies;
  DependencyResult[] failedDependencies;

  public LocalDateTime getPartitionTs() {
    return partitionTs;
  }

  public void setPartitionTs(LocalDateTime partitionTs) {
    this.partitionTs = partitionTs;
  }

  public boolean isProcessed() {
    return processed;
  }

  public void setProcessed(boolean processed) {
    this.processed = processed;
  }

  public boolean isResolved() {
    return resolved;
  }

  public void setResolved(boolean resolved) {
    this.resolved = resolved;
  }

  public DependencyResult[] getResolvedDependencies() {
    return resolvedDependencies;
  }

  public void setResolvedDependencies(DependencyResult[] resolvedDependencies) {
    this.resolvedDependencies = resolvedDependencies;
  }

  public DependencyResult[] getFailedDependencies() {
    return failedDependencies;
  }

  public void setFailedDependencies(DependencyResult[] failedDependencies) {
    this.failedDependencies = failedDependencies;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TablePartitionResult that = (TablePartitionResult) o;
    return processed == that.processed
        && resolved == that.resolved
        && Objects.equals(partitionTs, that.partitionTs)
        && Arrays.equals(resolvedDependencies, that.resolvedDependencies)
        && Arrays.equals(failedDependencies, that.failedDependencies);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(partitionTs, processed, resolved);
    result = 31 * result + Arrays.hashCode(resolvedDependencies);
    result = 31 * result + Arrays.hashCode(failedDependencies);
    return result;
  }

  @Override
  public String toString() {
    return "PartitionResult{"
        + "partitionTs="
        + partitionTs
        + ", processed="
        + processed
        + ", dependenciesResolved="
        + resolved
        + ", resolvedDependencies="
        + Arrays.toString(resolvedDependencies)
        + ", failedDependencies="
        + Arrays.toString(failedDependencies)
        + '}';
  }
}
