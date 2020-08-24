package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.S3;

public class ConfigS3Builder {
    private S3 s3;

    public ConfigS3Builder() {
        this.s3 = new S3();
    }

    public ConfigS3Builder Path(String path) {
        this.s3.setPath(path);
        return this;
    }

    public ConfigS3Builder SecretId(String secretId) {
        this.s3.setSecret_id(secretId);
        return this;
    }

    public ConfigS3Builder SecretKey(String secretKey) {
        this.s3.setSecret_key(secretKey);
        return this;
    }

    public S3 build(){
        this.s3.validate("");
        return s3;
    }
}
