package io.qimia.uhrwerk.common.metastore.config;

import com.google.common.base.Objects;
import io.qimia.uhrwerk.common.metastore.model.SecretModel;
import java.io.Serializable;

public class SecretResult implements Serializable {
  SecretModel newSecret;
  SecretModel oldSecret;
  boolean success;
  boolean error;
  String message;
  Exception exception;

  public SecretModel getNewSecret() {
    return newSecret;
  }

  public void setNewSecret(SecretModel newSecret) {
    this.newSecret = newSecret;
  }

  public SecretModel getOldSecret() {
    return oldSecret;
  }

  public void setOldSecret(SecretModel oldSecret) {
    this.oldSecret = oldSecret;
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
    if (!(o instanceof SecretResult)) {
      return false;
    }
    SecretResult that = (SecretResult) o;
    return isSuccess() == that.isSuccess() && isError() == that.isError()
        && Objects.equal(getNewSecret(), that.getNewSecret())
        && Objects.equal(getOldSecret(), that.getOldSecret())
        && Objects.equal(getMessage(), that.getMessage())
        && Objects.equal(getException(), that.getException());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getNewSecret(), getOldSecret(), isSuccess(), isError(), getMessage(),
        getException());
  }

  @Override
  public String toString() {
    return "SecretResult{" +
        "newSecret=" + newSecret +
        ", oldSecret=" + oldSecret +
        ", success=" + success +
        ", error=" + error +
        ", message='" + message + '\'' +
        ", exception=" + exception +
        '}';
  }
}
