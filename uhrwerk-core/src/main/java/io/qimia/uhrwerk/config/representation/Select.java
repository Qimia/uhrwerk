package io.qimia.uhrwerk.config.representation;


import io.qimia.uhrwerk.config.ConfigException;

public class Select extends Representation{

    private String query;
    private String column;

    public Select() {}

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

    @Override
    public void validate(String path){
        path += "select/";
        if(query == null){
            throw new ConfigException("Missing field: " + path + "query");
        }
        if(column == null){
            throw new ConfigException("Missing field: " + path + "column");
        }
    }
}
