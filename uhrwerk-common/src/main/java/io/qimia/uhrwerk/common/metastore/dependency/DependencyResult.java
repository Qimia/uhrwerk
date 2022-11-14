package io.qimia.uhrwerk.common.metastore.dependency;

import io.qimia.uhrwerk.common.model.ConnectionModel;
import io.qimia.uhrwerk.common.model.DependencyModel;
import io.qimia.uhrwerk.common.model.Partition;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;

public class DependencyResult implements Serializable {
  private static final long serialVersionUID = -1009786291331446408L;
  LocalDateTime partitionTs;
  boolean success;
  DependencyModel dependency;
  ConnectionModel Connection;
  LocalDateTime[] succeeded;
  LocalDateTime[] failed;
  Partition[] partitions;

  public LocalDateTime getPartitionTs() {
    return partitionTs;
  }

  public void setPartitionTs(LocalDateTime partitionTs) {
    this.partitionTs = partitionTs;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public DependencyModel getDependency() {
    return dependency;
  }

  public void setDependency(DependencyModel dependency) {
    this.dependency = dependency;
  }

  public ConnectionModel getConnection() {
    return Connection;
  }

  public void setConnection(ConnectionModel Connection) {
    this.Connection = Connection;
  }

  public LocalDateTime[] getSucceeded() {
    return succeeded;
  }

  public void setSucceeded(LocalDateTime[] succeeded) {
    this.succeeded = succeeded;
  }

  public LocalDateTime[] getFailed() {
    return failed;
  }

  public void setFailed(LocalDateTime[] failed) {
    this.failed = failed;
  }

  public Partition[] getPartitions() {
    return partitions;
  }

  public void setPartitions(Partition[] partitions) {
    this.partitions = partitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DependencyResult that = (DependencyResult) o;
    return success == that.success
        && Objects.equals(partitionTs, that.partitionTs)
        && Objects.equals(dependency, that.dependency)
        && Objects.equals(Connection, that.Connection)
        && Arrays.equals(succeeded, that.succeeded)
        && Arrays.equals(failed, that.failed)
        && Arrays.equals(partitions, that.partitions);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(partitionTs, success, dependency, Connection);
    result = 31 * result + Arrays.hashCode(succeeded);
    result = 31 * result + Arrays.hashCode(failed);
    result = 31 * result + Arrays.hashCode(partitions);
    return result;
  }

  @Override
  public String toString() {
    return "DependencyResult{"
        + "partitionTs="
        + partitionTs
        + ", success="
        + success
        + ", dependency="
        + dependency
        + ", connection="
        + Connection
        + ", succeeded="
        + Arrays.toString(succeeded)
        + ", failed="
        + Arrays.toString(failed)
        + ", partitions="
        + Arrays.toString(partitions)
        + '}';
  }
}
