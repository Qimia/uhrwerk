package io.qimia.uhrwerk.config.representation;

public class Connection {
    private String name;
    private JDBC jdbc;
    private S3 s3;
    private File file;

    public Connection(){}

    public JDBC getJdbc() {
        return jdbc;
    }

    public void setJdbc(JDBC jdbc) {
        this.jdbc = jdbc;
    }

    public S3 getS3() {
        return s3;
    }

    public void setS3(S3 s3) {
        this.s3 = s3;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
