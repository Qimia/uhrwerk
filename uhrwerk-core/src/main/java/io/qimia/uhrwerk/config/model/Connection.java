package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.config.ConnectionType;

import java.time.LocalDateTime;

public class Connection {

    private Long id;

    private String name = "";
    private String type = "";
    private String connectionUrl = "";
    private String jdbcDriver = "";
    private String user = "";
    private String pass = "";
    private String cloudId = "";
    private String cloudPass = "";
    private String cloudRegion = "";
    private String version = "1";

    private LocalDateTime createdTS;
    private LocalDateTime updatedTS;

    @Override
    public String toString() {
        return "Connection{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", jdbcUrl='" + connectionUrl + '\'' +
                ", jdbcDriver='" + jdbcDriver + '\'' +
                ", user='" + user + '\'' +
                ", pass='" + pass + '\'' +
                ", cloudId='" + cloudId + '\'' +
                ", cloudPass='" + cloudPass + '\'' +
                ", cloudRegion='" + cloudRegion + '\'' +
                ", version=" + version +
                '}';
    }

    public Connection() {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public ConnectionType getTypeEnum() {
        return ConnectionType.getConnectionType(type);
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public String getCloudId() {
        return cloudId;
    }

    public void setCloudId(String cloudId) {
        this.cloudId = cloudId;
    }

    public String getCloudPass() {
        return cloudPass;
    }

    public void setCloudPass(String cloudPass) {
        this.cloudPass = cloudPass;
    }

    public String getCloudRegion() {
        return cloudRegion;
    }

    public void setCloudRegion(String cloudRegion) {
        this.cloudRegion = cloudRegion;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public LocalDateTime getCreatedTS() {
        return createdTS;
    }

    public void setCreatedTS(LocalDateTime createdTS) {
        this.createdTS = createdTS;
    }

    public LocalDateTime getUpdatedTS() {
        return updatedTS;
    }

    public void setUpdatedTS(LocalDateTime updatedTS) {
        this.updatedTS = updatedTS;
    }
}
