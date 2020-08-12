package io.qimia.uhrwerk.config.representation;

public class Connection {
    protected String name;

    public Connection() {
    }

    public Connection(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
