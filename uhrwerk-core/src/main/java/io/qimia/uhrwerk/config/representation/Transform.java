package io.qimia.uhrwerk.config.representation;


public class Transform {

    private String type;
    private Partition[] partition;

    public Transform() {}

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Partition[] getPartition() {
        return partition;
    }

    public void setPartition(Partition[] partition) {
        this.partition = partition;
    }

}
