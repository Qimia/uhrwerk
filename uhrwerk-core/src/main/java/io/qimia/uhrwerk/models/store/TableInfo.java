package io.qimia.uhrwerk.models.store;

import javax.persistence.*;

@Entity
@Table( name = "tableinfo" )
public class TableInfo {

    private int id;
    private String path;
    private String area;
    private String vertical;
    private int version;
    private String connectionName;
    private ConnectionConfig connection;

//    public TableInfo() {}

    public TableInfo(
            String path,
            String area,
            String vertical,
            int version,
            String connectionName,
            ConnectionConfig connection
    ) {
        this.path = path;
        this.area = area;
        this.vertical = vertical;
        this.version = version;
        this.connectionName = connectionName;
        this.connection = connection;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Column
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Column
    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    @Column
    public String getVertical() {
        return vertical;
    }

    public void setVertical(String vertical) {
        this.vertical = vertical;
    }

    @Column
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Column
    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    @ManyToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="connectionId")
    public ConnectionConfig getConnection() {
        return connection;
    }

    public void setConnection(ConnectionConfig connection) {
        this.connection = connection;
    }

}
