package io.qimia.uhrwerk.models;

public enum ConnectionType {
    FS, JDBC, S3, GC, ABS;

    public static ConnectionType getConnectionType(String connection) {
        final ConnectionType res;
        switch (connection.toLowerCase()) {
            case "db":
            case "jdbc":
                res = JDBC;
                break;
            case "filesystem":
            case "fs":
                res = FS;
                break;
            case "s3":
                res = S3;
                break;
            case "abs":
            case "blobstorage":
                res = ABS;
                break;
            case "gc":
            case "cloudstorage":
                res = GC;
                break;
            default:
                System.err.println("Bad connection type given: " + connection);
                System.err.println("using filesystem instead");
                res = FS;
        }
        return res;
    }

    public boolean isSparkPath() {
        return ((this == S3) || (this == FS));
    }
}
