package io.qimia.uhrwerk.config.representation;


import io.qimia.uhrwerk.config.ConfigException;

public class ParallelLoad extends Representation{

    private String query;
    private String column;
    private Integer num;

    public ParallelLoad() {}

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public void validate(String path){
        path += "parallel_load/";
        if(query == null){
            throw new ConfigException("Missing field: " + path + "query");
        }
        if(column == null){
            throw new ConfigException("Missing field: " + path + "column");
        }
        if(num == 0){
            throw new ConfigException("Missing field: " + path + "num");
        }
    }
}
