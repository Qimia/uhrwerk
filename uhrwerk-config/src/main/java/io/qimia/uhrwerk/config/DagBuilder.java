package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Dag;
import io.qimia.uhrwerk.config.representation.*;

import java.util.ArrayList;

public class DagBuilder {
    private io.qimia.uhrwerk.config.representation.Dag dag;
    private ArrayList<Partition> partition;
    private ArrayList<Partition> source_partition;
    private ArrayList<Partition> dependency_partition;
    private ArrayList<ParallelLoad> parallel_load;
    private ArrayList<Select> select;
    private ArrayList<Transform> transform;
    private ArrayList<Table> tablesList;
    private ArrayList<Connection> connectionsList;
    private ArrayList<ArrayList<Source>> sourcesList;
    private ArrayList<ArrayList<Dependency>> dependenciesList;
    private ArrayList<ArrayList<Target>> targetsList;
    private Connection[] connections;
    private Table[] tables;
    private JDBC jdbc;
    private S3 s3;
    private File file;

    public DagBuilder() {
        this.dag = new io.qimia.uhrwerk.config.representation.Dag();
        this.tablesList = new ArrayList<Table>();
        this.connectionsList = new ArrayList<Connection>();
    }

    public DagBuilder name(String name) {
        this.connectionsList.get(this.connectionsList.size()-1).setName(name);
        return this;
    }

    public DagBuilder jdbc(JDBC jdbc) {
        this.connectionsList.get(this.connectionsList.size()-1).setJdbc(jdbc);
        return this;
    }

    public DagBuilder jdbc() {
        if (this.jdbc == null){
            this.jdbc = new JDBC();
        }
        this.connectionsList.get(this.connectionsList.size()-1).setJdbc(this.jdbc);
        return this;
    }

    public DagBuilder jdbc_url(String url) {
        if (this.connectionsList != null){
            if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setJdbc_url(url);
            } else {
                System.out.println("There is no JDBC object to which one can set a jdbc_url");
            }
        }
        return this;
    }

    public DagBuilder jdbc_driver(String driver) {
        if (this.connectionsList != null){
            if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setJdbc_driver(driver);
            } else {
                System.out.println("There is no JDBC object to which one can set a jdbc_driver");
            }
        }
        return this;
    }

    public DagBuilder user(String user) {
        if (this.connectionsList != null){
            if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setUser(user);
            } else {
                System.out.println("There is no JDBC object to which one can set a user");
            }
        }
        return this;
    }

    public DagBuilder pass(String pass) {
        if (this.connectionsList != null){
            if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setPass(pass);
            } else {
                System.out.println("There is no JDBC object to which one can set a pass");
            }
        }
        return this;
    }


    public DagBuilder s3(S3 s3) {
        this.connectionsList.get(this.connectionsList.size()-1).setS3(s3);
        return this;
    }

    public DagBuilder s3() {
        if (this.s3 == null){
            this.s3 = new S3();
        }
        this.connectionsList.get(this.connectionsList.size()-1).setS3(this.s3);
        return this;
    }



    public DagBuilder secret_id(String secret_id) {
        if (this.connectionsList != null){
            if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getS3().setSecret_id(secret_id);
            } else {
                System.out.println("There is no S3 object to which one can set a secret_id");
            }
        }
        return this;
    }

    public DagBuilder secret_key(String secret_key) {
        if (this.connectionsList != null){
            if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getS3().setSecret_key(secret_key);
            } else {
                System.out.println("There is no S3 object to which one can set a secret_key");
            }
        }
        return this;
    }


    public DagBuilder file(File file) {
        this.connectionsList.get(this.connectionsList.size()-1).setFile(file);
        return this;
    }

    public DagBuilder file() {
        if (this.file == null){
            this.file = new File();
        }
        this.connectionsList.get(this.connectionsList.size()-1).setFile(this.file);
        return this;
    }


    public DagBuilder connections(Connection[] connections) {
        this.dag.setConnections(connections);
        return this;
    }

    public DagBuilder connection() {
        Connection connection = new Connection();
        this.connectionsList.add(connection);
        return this;
    }

    public DagBuilder tables(Table[] tables) {
        this.dag.setTables(tables);
        return this;
    }

    public DagBuilder table() {
        Table table = new Table();
        this.tablesList.add(table);
        return this;
    }


    public DagBuilder area(String area) {
        if (this.tablesList != null) {
            if (this.tablesList.get(tablesList.size() - 1).getArea() == null) {
                this.tablesList.get(tablesList.size() - 1).setArea(area);
            }
            if (this.dependenciesList != null) {
                if (this.dependenciesList.get(tablesList.size()-1) != null){
                    if (this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).getArea() == null) {
                        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setArea(area);
                    }
            }
            }
        }
        return this;
    }

    public DagBuilder vertical(String vertical) {
        if (this.tablesList != null) {
            if (this.tablesList.get(tablesList.size() - 1).getVertical() == null) {
                this.tablesList.get(tablesList.size() - 1).setVertical(vertical);
            }
            if (this.dependenciesList != null) {
                if (this.dependenciesList.get(tablesList.size() - 1) != null) {
                    if (this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).getVertical() == null) {
                        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setVertical(vertical);
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder table(String table) {
        if (this.tablesList != null) {
            if (this.tablesList.get(tablesList.size() - 1).getTable() == null) {
                this.tablesList.get(tablesList.size() - 1).setTable(table);
            }
            if (this.dependenciesList != null) {
                if (this.dependenciesList.get(tablesList.size() - 1) != null) {
                    if (this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).getTable() == null) {
                        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setTable(table);
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder version(String version) {
        if (this.tablesList != null) {
            if (this.tablesList.get(tablesList.size() - 1).getVersion() == null) {
                this.tablesList.get(tablesList.size() - 1).setVersion(version);
            }
            if (this.sourcesList != null) {
                if (this.sourcesList.get(tablesList.size() - 1) != null) {
                    if (this.sourcesList.get(tablesList.size()-1).get(sourcesList.get(tablesList.size()-1).size() - 1).getVersion() == null) {
                        this.sourcesList.get(tablesList.size()-1).get(sourcesList.get(tablesList.size()-1).size() - 1).setVersion(version);
                    }
                }
            }

            if (this.dependenciesList != null) {
                if (this.dependenciesList.get(tablesList.size() - 1) != null) {
                    if (this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).getVersion() == null) {
                        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setVersion(version);
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder parallelism(int parallelism) {
        if (this.tablesList != null) {
            this.tablesList.get(tablesList.size()-1).setParallelism(parallelism);
        }
        return this;
    }

    public DagBuilder maxBulkSize(int maxBulkSize) {
        if (this.tablesList != null) {
            this.tablesList.get(tablesList.size()-1).setMax_bulk_size(maxBulkSize);
        }
        return this;
    }

    public DagBuilder partition(io.qimia.uhrwerk.config.representation.Partition partition) {
        if(this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size() - 1).getPartition() == null) {
                this.tablesList.get(tablesList.size() - 1).setPartition(partition);
            }

            if (this.sourcesList != null) {
                if (this.sourcesList.get(tablesList.size() - 1) != null) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition() == null) {
                        this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setPartition(partition);
                    }
                }
            }
            if (this.dependenciesList != null) {
                if (this.dependenciesList.get(tablesList.size() - 1) != null) {
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                        if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition() == null) {
                            this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().setPartition(partition);
                        }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder partition() {
        if (this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size()-1).getPartition() == null) {
                if (this.partition == null){
                    this.partition = new ArrayList<Partition>();
                }
                this.partition.add(new Partition());
            }
            if (this.sourcesList != null) {
                if (this.sourcesList.get(tablesList.size() - 1) != null) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition() == null) {
                        if (this.source_partition == null){
                            this.source_partition = new ArrayList<Partition>();
                        }
                        this.source_partition.add(new io.qimia.uhrwerk.config.representation.Partition());
                    }
                }
            }
            if (this.dependenciesList != null) {
                if (this.dependenciesList.get(tablesList.size() - 1) != null) {
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                        if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition() == null) {
                            if (this.dependency_partition == null){
                                this.dependency_partition = new ArrayList<Partition>();
                            }
                            this.dependency_partition.add(new io.qimia.uhrwerk.config.representation.Partition());
                        }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder unit(String unit) {
        if (this.partition == null && this.source_partition == null && this.dependency_partition == null){
            System.out.println("There is no partition defined to which a partition unit can be set!");
        }
        if (this.partition != null) {
            if (this.partition.get(tablesList.size() - 1) != null) {
                if (this.partition.get(tablesList.size() - 1).getUnit() == null) {
                    this.partition.get(tablesList.size() - 1).setUnit(unit);
                    this.tablesList.get(tablesList.size() - 1).setPartition(this.partition.get(tablesList.size() - 1));
                }
            }
        }
        if (this.source_partition != null) {
            if (this.source_partition.get(tablesList.size() - 1) != null) {
                if (this.source_partition.get(tablesList.size() - 1).getUnit() == null) {
                    this.source_partition.get(tablesList.size() - 1).setUnit(unit);
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setPartition(this.source_partition.get(tablesList.size() - 1));
                }
            }
        }
        if (this.dependency_partition != null) {
            if (this.dependency_partition.get(tablesList.size() - 1) != null) {
                if (this.dependency_partition.get(tablesList.size() - 1).getUnit() == null) {
                    this.dependency_partition.get(tablesList.size() - 1).setUnit(unit);
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                        this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().setPartition(this.dependency_partition.get(tablesList.size() - 1));
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder size(int size) {
        if (this.partition == null && this.source_partition == null && this.dependency_partition == null){
            System.out.println("There is no partition defined to which a partition size can be set!");
        }
        if (this.partition != null) {
            if (this.partition.get(tablesList.size() - 1) != null) {
                if (this.partition.get(tablesList.size() - 1).getSize() == null) {
                    this.partition.get(tablesList.size() - 1).setSize(size);
                    this.tablesList.get(tablesList.size() - 1).setPartition(this.partition.get(tablesList.size() - 1));
                }
            }
        }
        if (this.source_partition != null) {
            if (this.source_partition.get(tablesList.size() - 1) != null) {
                if (this.source_partition.get(tablesList.size() - 1).getSize() == null) {
                    this.source_partition.get(tablesList.size() - 1).setSize(size);
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setPartition(this.source_partition.get(tablesList.size() - 1));
                }
            }
        }
        if (this.dependency_partition != null) {
            if (this.dependency_partition.get(tablesList.size() - 1) != null) {
                if (this.dependency_partition.get(tablesList.size() - 1).getSize() == null) {
                    this.dependency_partition.get(tablesList.size() - 1).setSize(size);
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                        this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().setPartition(this.dependency_partition.get(tablesList.size() - 1));
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder sources(io.qimia.uhrwerk.config.representation.Source[] sources) {
        if (this.tablesList != null) {
            this.tablesList.get(tablesList.size() - 1).setSources(sources);
        }
        return this;
    }

    public DagBuilder source() {
            if (this.sourcesList == null) {
                this.sourcesList = new ArrayList<ArrayList<io.qimia.uhrwerk.config.representation.Source>>();
            }
            Source source = new io.qimia.uhrwerk.config.representation.Source();
            this.sourcesList.get(tablesList.size() - 1).add(source);
        return this;
    }

    public DagBuilder targets(io.qimia.uhrwerk.config.representation.Target[] targets) {
        if (this.tablesList != null) {
            this.tablesList.get(tablesList.size() - 1).setTargets(targets);
        }
        return this;
    }

    public DagBuilder target() {
        if (this.tablesList != null) {
            if (this.targetsList == null) {
                this.targetsList = new ArrayList<ArrayList<io.qimia.uhrwerk.config.representation.Target>>();
            }
            io.qimia.uhrwerk.config.representation.Target target = new io.qimia.uhrwerk.config.representation.Target();
            this.targetsList.get(tablesList.size() - 1).add(target);
        }
        return this;
    }

    public DagBuilder connection_name(String connection_name) {
        if (this.sourcesList != null) {
            if (this.sourcesList.get(tablesList.size() - 1) != null) {
                if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getConnection_name() == null) {
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setConnection_name(connection_name);
                }
            }
        }
        if (this.targetsList != null) {
            if (this.targetsList.get(tablesList.size() - 1) != null) {
                if (this.targetsList.get(tablesList.size() - 1).get(targetsList.get(tablesList.size() - 1).size() - 1).getConnection_name() == null) {
                    this.targetsList.get(tablesList.size() - 1).get(targetsList.get(tablesList.size() - 1).size() - 1).setConnection_name(connection_name);
                }
            }
        }
        return this;
    }

    public DagBuilder path(String path) {
        if (this.connectionsList.get(this.connectionsList.size()-1).getS3() == null
                && this.connectionsList.get(this.connectionsList.size()-1).getFile() == null
        && this.sourcesList.get(tablesList.size() - 1) == null){
            System.out.println("There is no S3 or File or Source object to which one can set a path");
        }
        if (this.connectionsList != null){
            if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null) {
                if (this.connectionsList.get(this.connectionsList.size() - 1).getS3().getPath() == null) {
                    this.connectionsList.get(this.connectionsList.size() - 1).getS3().setPath(path);
                }
            }
            if (this.connectionsList.get(this.connectionsList.size()-1).getFile() != null) {
                if (this.connectionsList.get(this.connectionsList.size() - 1).getFile().getPath() == null) {
                    this.connectionsList.get(this.connectionsList.size() - 1).getFile().setPath(path);
                }
            }
        }
        if (this.sourcesList != null) {
            if (this.sourcesList.get(tablesList.size() - 1) != null) {
                this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setPath(path);
            }
        }
        return this;
    }


    public DagBuilder format(String format) {
        if (this.sourcesList != null) {
            if (this.sourcesList.get(tablesList.size() - 1) != null) {
                if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getFormat() == null) {
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setFormat(format);
                }
            }
        }
        if (this.targetsList != null) {
            if (this.targetsList.get(tablesList.size() - 1) != null) {
                if (this.targetsList.get(tablesList.size() - 1).get(targetsList.get(tablesList.size() - 1).size() - 1).getFormat() == null) {
                    this.targetsList.get(tablesList.size() - 1).get(targetsList.get(tablesList.size() - 1).size() - 1).setFormat(format);
                }
            }
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(tablesList.size() - 1) != null) {
                if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getFormat() == null) {
                    this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).setFormat(format);
                }
            }
        }
        return this;
    }


    public DagBuilder parallel_load(ParallelLoad parallel_load) {
        if (this.sourcesList != null) {
            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setParallel_load(parallel_load);
        }
        return this;
    }

    public DagBuilder parallel_load() {
        if (this.parallel_load == null) {
            this.parallel_load = new ArrayList<ParallelLoad>();
        }
        this.parallel_load.add( new ParallelLoad());
        return this;
    }


    public DagBuilder query(String query) {
        if (this.parallel_load == null && this.select == null){
            System.out.println("There is no parallel_load or select defined to which a query can be set!");
        }
        if (this.parallel_load != null) {
            if (this.parallel_load.get(tablesList.size()-1) != null) {
                if (this.parallel_load.get(tablesList.size()-1).getQuery() == null) {
                    this.parallel_load.get(tablesList.size()-1).setQuery(query);
                    this.sourcesList.get(tablesList.size()-1).get(sourcesList.get(tablesList.size()-1).size() - 1).setParallel_load(this.parallel_load.get(tablesList.size()-1));
                }
            }
        }
        if (this.select != null) {
            if (this.select.get(tablesList.size()-1) != null) {
                if (this.select.get(tablesList.size() - 1).getQuery() == null) {
                    this.select.get(tablesList.size() - 1).setQuery(query);
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setSelect(this.select.get(tablesList.size() - 1));
                }
            }
        }
        return this;
    }

    public DagBuilder column(String column) {
        if (this.parallel_load == null && this.select == null){
            System.out.println("There is no parallel_load or select defined to which a column can be set!");
        }
        if (this.parallel_load != null) {
            if (this.parallel_load.get(tablesList.size()-1) != null) {
                if (this.parallel_load.get(tablesList.size()-1).getColumn() == null) {
                    this.parallel_load.get(tablesList.size()-1).setColumn(column);
                    this.sourcesList.get(tablesList.size()-1).get(sourcesList.get(tablesList.size()-1).size() - 1).setParallel_load(this.parallel_load.get(tablesList.size()-1));
                }
            }
        }
        if (this.select != null) {
            if (this.select.get(tablesList.size()-1) != null) {
                if (this.select.get(tablesList.size() - 1).getColumn() == null) {
                    this.select.get(tablesList.size() - 1).setColumn(column);
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setSelect(this.select.get(tablesList.size() - 1));
                }
            }
        }
        return this;
    }

    public DagBuilder num(int num) {
        if (this.parallel_load.get(tablesList.size() - 1) != null) {
            this.parallel_load.get(tablesList.size() - 1).setNum(num);
            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setParallel_load(this.parallel_load.get(tablesList.size() - 1));
        }
        return this;
    }

    public DagBuilder select(Select select) {
        if (this.sourcesList.get(tablesList.size() - 1) != null) {
            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setSelect(select);
        }
        return this;
    }

    public DagBuilder select() {
        if (this.select == null){
            this.select = new ArrayList<Select>();
        }
        this.select.add(new Select());
        return this;
    }

    public DagBuilder dependencies(io.qimia.uhrwerk.config.representation.Dependency[] dependencies) {
        this.tablesList.get(tablesList.size()-1).setDependencies(dependencies);
        return this;
    }

    public DagBuilder dependency() {
        if (this.dependenciesList == null) {
            this.dependenciesList = new ArrayList<ArrayList<io.qimia.uhrwerk.config.representation.Dependency>>();
        }
        io.qimia.uhrwerk.config.representation.Dependency dependency = new io.qimia.uhrwerk.config.representation.Dependency();
        this.dependenciesList.get(tablesList.size()-1).add(dependency);
        return this;
    }

    public DagBuilder transform(Transform transform) {
        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setTransform(transform);
        return this;
    }

    public DagBuilder transform() {
        if (this.transform == null) {
            this.transform = new ArrayList<Transform>();
        }
        this.transform.add(new Transform());
        return this;
    }

    public DagBuilder type(String type) {
        this.transform.get(tablesList.size()-1).setType(type);
        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setTransform(this.transform.get(tablesList.size()-1));
        return this;
    }

    public Dag build() {
        if (tablesList != null) {
            int tablesLength = tablesList.size();
            System.out.println(tablesLength);
            if (tablesLength != 0) {
                for (int i = 0; i < tablesLength; i++) {
                    if (this.sourcesList != null) {
                        Source[] sources = new Source[sourcesList.get(i).size()];
                        sourcesList.get(i).toArray(sources);
                        this.tablesList.get(i).setSources(sources);
                    }
                    if (this.targetsList != null) {
                        Target[] targets = new Target[targetsList.get(i).size()];
                        targetsList.get(i).toArray(targets);
                        this.tablesList.get(i).setTargets(targets);
                    }
                    if (this.dependenciesList != null) {
                        Dependency[] dependencies = new Dependency[dependenciesList.get(i).size()];
                        dependenciesList.get(i).toArray(dependencies);
                        this.tablesList.get(i).setDependencies(dependencies);
                    }
                }
                this.tables = new Table[tablesList.size()];
                tablesList.toArray(this.tables);
                this.dag.setTables(this.tables);
            }
        }
        if (this.connectionsList != null) {
            this.connections = new Connection[connectionsList.size()];
            connectionsList.toArray(this.connections);
            this.dag.setConnections(this.connections);
        }
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelDag(this.dag);
    }
}
