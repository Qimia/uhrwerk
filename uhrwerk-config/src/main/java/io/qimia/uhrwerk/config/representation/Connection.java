package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class Connection{
    private String name;
    private JDBC jdbc;
    private S3 s3;
    private File file;
    //private static String[] allowedFormats = new String[]{"jdbc","s3","file"};

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



    public void validate(String path){
        path += "connection/";
        int containedFormats = 0;
        if(name == null){
            throw new ConfigException("Missing field: " + path + "name");
        }
        if(jdbc == null && s3 == null && file == null){
            throw new ConfigException("Missing connection format: choose either jdbc/s3/file under path: " + path);
        }

        if(jdbc != null && s3 != null){
            throw new ConfigException("Only one connection format at the same time: jdbc/s3/file under path: " + path);
        }
        if(jdbc != null && file != null){
            throw new ConfigException("Only one connection format at the same time: jdbc/s3/file under path: " + path);
        }
        if(s3 != null && file != null){
            throw new ConfigException("Only one connection format at the same time: jdbc/s3/file under path: " + path);
        }

    }

    @Override
    public String toString() {
        return "Source{" +
                "name='" + name + '\'' +
                ", jdbc='" + jdbc + '\'' +
                ", s3='" + s3 + '\'' +
                ", file='" + file + '\'' +
                '}';
    }

}
