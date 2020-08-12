package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Select;

public class SelectBuilder {
    private String query;
    private String column;

    public SelectBuilder withQuery(String query) {
        this.query = query;
        return this;
    }

    public SelectBuilder withColumn(String column) {
        this.column = column;
        return this;
    }

    public Select build() {
        return new Select();
    }
}

