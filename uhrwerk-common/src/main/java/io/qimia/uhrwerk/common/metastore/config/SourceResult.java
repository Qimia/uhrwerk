package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Source;

public class SourceResult {
    Source newResult;
    Source oldResult;
    boolean success;
    boolean error;
    String message;
    Exception exception;

    public Source getNewResult() {
        return newResult;
    }

    public void setNewResult(Source newResult) {
        this.newResult = newResult;
    }

    public Source getOldResult() {
        return oldResult;
    }

    public void setOldResult(Source oldResult) {
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
}
