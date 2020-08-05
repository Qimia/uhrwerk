package io.qimia.uhrwerk.models.config;

public interface InTable extends Table {
    public String getPartitionQuery();
    public void setPartitionQuery(String partitionQuery);
    public String getPartitionColumn();
    public void setPartitionColumn(String partitionColumn);
    public String getSelectQuery();
    public void setSelectQuery(String selectQuery);
    public String getQueryColumn();
    public void setQueryColumn(String queryColumn);
}
