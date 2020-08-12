package io.qimia.uhrwerk.config.representation;


public class ConnectionName {

    private String area;
    private String vertical;
    private String table;
    private String format;
    private String version;
    private Transform[] transform;
    private String transform_type;

    public ConnectionName() {}

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getVertical() {
        return vertical;
    }

    public void setVertical(String vertical) {
        this.vertical = vertical;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Transform[] getTransform() {
        return transform;
    }

    public void setTransform(Transform[] transform) {
        this.transform = transform;
    }

    public String getTransform_type() {
        return transform_type;
    }

    public void setTransform_type(String transform_type) {
        this.transform_type = transform_type;
    }
}
