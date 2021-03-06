package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Dependency;

import java.io.Serializable;

public class DependencyStoreResult implements Serializable {
    private static final long serialVersionUID = 2456291293896086088L;
    Dependency[] dependenciesSaved;
    boolean success;
    boolean error;
    String message;
    Exception exception;

    public Dependency[] getDependenciesSaved() {
        return dependenciesSaved;
    }

    public void setDependenciesSaved(Dependency[] dependenciesSaved) {
        this.dependenciesSaved = dependenciesSaved;
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
