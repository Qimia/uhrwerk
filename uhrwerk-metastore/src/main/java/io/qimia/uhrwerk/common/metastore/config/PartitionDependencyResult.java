package io.qimia.uhrwerk.common.metastore.config;

import java.io.Serializable;

public class PartitionDependencyResult implements Serializable {
    private static final long serialVersionUID = -8972593554921481622L;
    boolean success;
    boolean error;
    String message;
    Exception exception;

    public PartitionDependencyResult() {}

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
}
