package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Partition;

public class PartitionResult {
    Partition newResult;
    Partition oldResult;
    boolean success;
    boolean error;
    String message;
    Exception exception;

    public Partition getNewResult() {
        return newResult;
    }

    public void setNewResult(Partition newResult) {
        this.newResult = newResult;
    }

    public Partition getOldResult() {
        return oldResult;
    }

    public void setOldResult(Partition oldResult) {
        this.oldResult = oldResult;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
        this.error = !success;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
        this.success = !error;
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