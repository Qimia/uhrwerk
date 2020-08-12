package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.File;
import io.qimia.uhrwerk.config.representation.JDBC;
import io.qimia.uhrwerk.config.representation.S3;

public class ConnectionBuilder {
    private String name;
    private JDBC jdbc;
    private S3 s3;
    private File file;

    public ConnectionBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public ConnectionBuilder withJdbc(JDBC jdbc) {
        this.jdbc = jdbc;
        return this;
    }

    public ConnectionBuilder withS3(S3 s3) {
        this.s3 = s3;
        return this;
    }

    public ConnectionBuilder withFile(File file) {
        this.file = file;
        return this;
    }

    public Connection build(){
        return new Connection();
    }
}

