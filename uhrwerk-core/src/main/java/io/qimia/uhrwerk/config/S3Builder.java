package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.S3;

public class S3Builder {
    private String path;
    private String secretId;
    private String secretKey;

    public S3Builder withPath(String path) {
        this.path = path;
        return this;
    }

    public S3Builder withSecretId(String secretId) {
        this.secretId = secretId;
        return this;
    }

    public S3Builder withSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public S3 build(){
     return new S3();
    }
}
