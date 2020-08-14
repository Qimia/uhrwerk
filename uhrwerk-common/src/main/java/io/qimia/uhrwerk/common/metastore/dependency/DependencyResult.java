package io.qimia.uhrwerk.common.metastore.dependency;

import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.Dependency;
import io.qimia.uhrwerk.common.model.Partition;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;

public class DependencyResult {
  LocalDateTime partitionTs;
  boolean success;
  Dependency dependency;
  Connection connection;
  Partition[] succeeded;
  Partition[] failed;

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
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

  public Partition[] getFailed() {
    return failed;
  }

  public void setFailed(Partition[] failed) {
    this.failed = failed;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DependencyResult that = (DependencyResult) o;
    return success == that.success
        && Objects.equals(partitionTs, that.partitionTs)
        && Objects.equals(dependency, that.dependency)
        && Objects.equals(connection, that.connection)
        && Arrays.equals(succeeded, that.succeeded)
        && Arrays.equals(failed, that.failed);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(partitionTs, success, dependency, connection);
    result = 31 * result + Arrays.hashCode(succeeded);
    result = 31 * result + Arrays.hashCode(failed);
    return result;
  }

  @Override
  public String toString() {
    return "DependencyResult{"
        + "dependentPartitionTs="
        + partitionTs
        + ", success="
        + success
        + ", dependency="
        + dependency
        + ", connection="
        + connection
        + ", successPartitions="
        + Arrays.toString(succeeded)
        + ", failedPartitions="
        + Arrays.toString(failed)
        + '}';
  }
}
