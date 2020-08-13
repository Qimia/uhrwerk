package io.qimia.uhrwerk.config.representation;


public class ParallelLoad {

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

}
