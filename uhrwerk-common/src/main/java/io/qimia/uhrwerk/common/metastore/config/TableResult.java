package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.TableModel;

import java.io.Serializable;

public class TableResult implements Serializable {
    private static final long serialVersionUID = 2402210215093160756L;
    private TableModel newResult;
    private TableModel oldResult;
    private boolean success;
    private boolean error;
    private String message;
    private Exception exception;
    private TargetResult targetResult;
    private SourceResult[] sourceResults;
    private DependencyStoreResult dependencyResult;

    public TargetResult getTargetResult() {
        return targetResult;
    }

    public SourceResult[] getSourceResults() {
        return sourceResults;
    }

    public DependencyStoreResult getDependencyResult() {
        return dependencyResult;
    }

    public TableModel getNewResult() {
        return newResult;
    }

    public void setNewResult(TableModel newResult) {
        this.newResult = newResult;
    }

    public TableModel getOldResult() {
        return oldResult;
    }

    public void setOldResult(TableModel oldResult) {
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

    public void setTargetResult(TargetResult targetResult) {
        this.targetResult = targetResult;
    }

    public void setSourceResults(SourceResult[] sourceResults) {
        this.sourceResults = sourceResults;
    }

    public void setDependencyResult(DependencyStoreResult dependencyResult) {
        this.dependencyResult = dependencyResult;
    }
}
