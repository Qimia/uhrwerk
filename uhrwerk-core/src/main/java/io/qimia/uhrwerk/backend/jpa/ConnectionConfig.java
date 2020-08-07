package io.qimia.uhrwerk.backend.jpa;

import javax.persistence.*;

@Entity
@Table( name = "connectionconfigs" )
public class ConnectionConfig {

    private int id;
    private String name;
    private String type;
    private String url;
    private int version;

    public ConnectionConfig() {}

    public ConnectionConfig(String name, String type, String url, int version) {
        this.name = name;
        this.type = type;
        this.url = url;
        this.version = version;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Column
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Column
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Column
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Column
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
