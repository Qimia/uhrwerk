package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.config.representation.S3;

public class S3Builder {
  private String path;
  private String secretId;
  private String secretKey;
  private ConnectionBuilder parent;

  public S3Builder() {}

  public S3Builder(ConnectionBuilder parent) {
    this.parent = parent;
  }

  public S3Builder path(String path) {
    this.path = path;
    return this;
  }

  public S3Builder secretId(String secretId) {
    this.secretId = secretId;
    return this;
  }

  public S3Builder secretKey(String secretKey) {
    this.secretKey = secretKey;
    return this;
  }

  public ConnectionBuilder done() {
    this.parent.s3(this.build());
    return this.parent;
  }

  public S3 build() {
    var s3 = new S3();
    s3.setPath(this.path);
    s3.setSecretId(this.secretId);
    s3.setSecretKey(this.secretKey);
    s3.validate("");
    return s3;
  }
}
