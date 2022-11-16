package io.qimia.uhrwerk.common.metastore.dependency;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Arrays;

public class TablePartitionResultSet implements Serializable {
  private static final long serialVersionUID = -3922367389127277814L;
  LocalDateTime[] resolvedTs;
  LocalDateTime[] processedTs;
  LocalDateTime[] failedTs;

  TablePartitionResult[] resolved;
  TablePartitionResult[] processed;
  TablePartitionResult[] failed;

  public LocalDateTime[] getResolvedTs() {
    return resolvedTs;
  }

  public void setResolvedTs(LocalDateTime[] resolvedTs) {
    this.resolvedTs = resolvedTs;
  }

  public LocalDateTime[] getProcessedTs() {
    return processedTs;
  }

  public void setProcessedTs(LocalDateTime[] processedTs) {
    this.processedTs = processedTs;
  }

  public LocalDateTime[] getFailedTs() {
    return failedTs;
  }

  public void setFailedTs(LocalDateTime[] failedTs) {
    this.failedTs = failedTs;
  }

  public TablePartitionResult[] getResolved() {
    return resolved;
  }

  public void setResolved(TablePartitionResult[] resolved) {
    this.resolved = resolved;
  }

  public TablePartitionResult[] getProcessed() {
    return processed;
  }

  public void setProcessed(TablePartitionResult[] processed) {
    this.processed = processed;
  }

  public TablePartitionResult[] getFailed() {
    return failed;
  }

  public void setFailed(TablePartitionResult[] failed) {
    this.failed = failed;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TablePartitionResultSet that = (TablePartitionResultSet) o;
    return Arrays.equals(resolvedTs, that.resolvedTs)
        && Arrays.equals(processedTs, that.processedTs)
        && Arrays.equals(failedTs, that.failedTs)
        && Arrays.equals(resolved, that.resolved)
        && Arrays.equals(processed, that.processed)
        && Arrays.equals(failed, that.failed);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(resolvedTs);
    result = 31 * result + Arrays.hashCode(processedTs);
    result = 31 * result + Arrays.hashCode(failedTs);
    result = 31 * result + Arrays.hashCode(resolved);
    result = 31 * result + Arrays.hashCode(processed);
    result = 31 * result + Arrays.hashCode(failed);
    return result;
  }

  @Override
  public String toString() {
    return "TableResult{"
        + "resolvedTs="
        + Arrays.toString(resolvedTs)
        + ", processedTs="
        + Arrays.toString(processedTs)
        + ", failedTs="
        + Arrays.toString(failedTs)
        + ", resolved="
        + Arrays.toString(resolved)
        + ", processed="
        + Arrays.toString(processed)
        + ", failed="
        + Arrays.toString(failed)
        + '}';
  }
}
