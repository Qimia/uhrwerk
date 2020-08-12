package io.qimia.uhrwerk.config.representation;


public class Select {

    private String query;
    private String column;

    public Select() {}

    public String getQuery() {
        return query;
    }

    public void setQuery(String unit) {
        this.query = query;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String columns) {
        this.column = column;
    }

}
