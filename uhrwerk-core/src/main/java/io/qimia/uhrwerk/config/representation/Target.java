package io.qimia.uhrwerk.config.representation;


public class Target extends Representation{

    private String connection_name;
    private String format;

    public Target() {}

    public String getConnection_name() {
        return connection_name;
    }

    public void setConnection_name(String connection_name) {
        this.connection_name = connection_name;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

}
