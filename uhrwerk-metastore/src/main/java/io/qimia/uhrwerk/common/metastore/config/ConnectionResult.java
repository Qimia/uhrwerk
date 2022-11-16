package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel;

import java.io.Serializable;
import java.util.Objects;

public class ConnectionResult implements Serializable {
  private static final long serialVersionUID = 3800840086237687091L;
  ConnectionModel newConnection;
  ConnectionModel oldConnection;
  boolean success;
  boolean error;
  String message;
  Exception exception;

  public ConnectionModel getNewConnection() {
    return newConnection;
  }

  public void setNewConnection(ConnectionModel newConnection) {
    this.newConnection = newConnection;
  }

  public ConnectionModel getOldConnection() {
    return oldConnection;
  }

  public void setOldConnection(ConnectionModel oldConnection) {
    this.oldConnection = oldConnection;
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConnectionResult that = (ConnectionResult) o;
    return success == that.success
        && error == that.error
        && Objects.equals(newConnection, that.newConnection)
        && Objects.equals(oldConnection, that.oldConnection)
        && Objects.equals(message, that.message)
        && Objects.equals(exception, that.exception);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newConnection, oldConnection, success, error, message, exception);
  }

  @Override
  public String toString() {
    return "ConnectionResult{"
        + "newConnection="
        + newConnection
        + ", oldConnection="
        + oldConnection
        + ", success="
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
