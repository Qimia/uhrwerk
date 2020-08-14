package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.ParallelLoad;

public class ParallelLoadBuilder {
    private String query;
    private String column;
    private Integer num;

    public ParallelLoadBuilder withQuery(String query) {
        this.query = query;
        return this;
    }

    public ParallelLoadBuilder withColumn(String column) {
        this.column = column;
        return this;
    }

    public ParallelLoadBuilder withNum(Integer num) {
        this.num = num;
        return this;
    }

    public ParallelLoad build(){
        ParallelLoad parallelLoad = new ParallelLoad();
        parallelLoad.setColumn(this.column);
        parallelLoad.setNum(this.num);
        parallelLoad.setQuery(this.query);
        return parallelLoad;
    }
}
