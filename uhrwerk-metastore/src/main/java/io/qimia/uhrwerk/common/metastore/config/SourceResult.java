package io.qimia.uhrwerk.common.metastore.config;

import com.google.common.base.Objects;
import io.qimia.uhrwerk.common.metastore.model.SourceModel2;
import java.io.Serializable;

public class SourceResult implements Serializable {
  private static final long serialVersionUID = 5997084266031599153L;
  SourceModel2 newResult;
  SourceModel2 oldResult;
  boolean success;
  boolean error;
  String message;
  Exception exception;

  public SourceModel2 getNewResult() {
    return newResult;
  }

  public void setNewResult(SourceModel2 newResult) {
    this.newResult = newResult;
  }

  public SourceModel2 getOldResult() {
    return oldResult;
  }

  public void setOldResult(SourceModel2 oldResult) {
    this.oldResult = oldResult;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public boolean isError() {
    return error;
  }

  public void setError(boolean error) {
    this.error = error;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Exception getException() {
    return exception;
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceResult)) {
      return false;
    }
    SourceResult that = (SourceResult) o;
    return isSuccess() == that.isSuccess()
        && isError() == that.isError()
        && Objects.equal(getNewResult(), that.getNewResult())
        && Objects.equal(getOldResult(), that.getOldResult())
        && Objects.equal(getMessage(), that.getMessage())
        && Objects.equal(getException(), that.getException());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getNewResult(), getOldResult(), isSuccess(), isError(), getMessage(), getException());
  }

  @Override
  public String toString() {
    return "SourceResult{"
        + "success="
        + success
        + ", error="
        + error
        + ", message='"
        + message
        + '\''
        + ", exception="
        + exception
        + '}';
  }
}
