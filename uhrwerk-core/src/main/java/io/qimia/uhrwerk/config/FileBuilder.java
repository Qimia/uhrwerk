package io.qimia.uhrwerk.config;


import io.qimia.uhrwerk.config.representation.File;

public class FileBuilder {
    private String path;

    public FileBuilder withPath(String path) {
        this.path = path;
        return this;
    }

    public File build(){
        File file = new File();
        file.setPath(this.path);
        return file;
    }
}
