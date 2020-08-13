package io.qimia.uhrwerk.config.representation;


public class Partition extends Representation{

    private String unit;
    private Integer size;

    public Partition() {}

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

}
