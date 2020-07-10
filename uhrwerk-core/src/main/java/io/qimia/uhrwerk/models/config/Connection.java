package io.qimia.uhrwerk.models.config;

import io.qimia.uhrwerk.models.ConnectionType;

public class Connection {

    private String name = "";
    private String type = "";
    private String jdbcUri = "";
    private String user = "";
    private String pass = "";
    private String cloudId = "";
    private String cloudPass = "";
    private String cloudRegion = "";
    private String startPath = "";
    private int version = 1;

    public Connection() {}

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

    public String getJdbcUri() {
        return jdbcUri;
    }

    public void setJdbcUri(String jdbcUri) {
        this.jdbcUri = jdbcUri;
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

    public String getStartPath() {
        return startPath;
    }

    public void setStartPath(String startPath) {
        this.startPath = startPath;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
