package io.qimia.uhrwerk.config.representation;


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

}
