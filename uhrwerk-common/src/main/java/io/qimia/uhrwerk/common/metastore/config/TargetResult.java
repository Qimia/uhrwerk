package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.TargetModel;

import java.io.Serializable;

public class TargetResult implements Serializable {

    private static final long serialVersionUID = 740323809677539485L;
    TargetModel[] storedTargets;
    boolean success;
    boolean error;
    String message;
    Exception exception;

    public TargetModel[] getStoredTargets() {
        return storedTargets;
    }

    public void setStoredTargets(TargetModel[] storedTargets) {
        this.storedTargets = storedTargets;
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
}
