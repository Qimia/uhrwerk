# Qimia Uhrwerk
A data-dependency manager, data-pipeline scheduler, and data-lake build tool.

## Modules

- Uhrwerk-common: Model Pojo's and module-interfaces
- Uhrwerk-backend-mysql: Database access objects which implement persistence- and dependency-interfaces
- Uhrwerk-config: Configuration by loading yaml files or by using java-builders
- Uhrwerk-framemanager: implementation of the load/read dataframe interface.
- Uhrwerk-engine: Management of Tables and methods to execute tasks
- Uhrwerk-example: Example usage of Uhrwerk
- Uhrwerk-cli: Uhrwerk's CLI

## Testing
For quick unit tests run `mvn -DtagsToExclude=io.qimia.uhrwerk.tags.DbTest,io.qimia.uhrwerk.tags.Slow test` to disable Mysql dependent and slower tests.

## Setup

Uhrwerk needs two things to run:
  * A metastore running on a mysql docker container.
  * A Spark installation (either locally or using docker).
  

1. Start docker-compose
  
    1.1 For a local Spark installation: Run `./start-docker.sh metastore-only` to start up docker-compose with only the metastore mysql container and an adminer. 
    
    1.2 With no local Spark installation:
    * Install Docker (Docker Desktop recommended for ease of use)
    * Run `./build-spark-docker-images.sh` to build the necessary Spark docker images. This takes a while.
    * Run `./start-docker.sh all` to start up docker-compose with both the metastore and spark containers (by default one master and one worker).

2. (Only necessary when running for the first time): 
    * Install MySQL Client (mysql-client package)
    * Initialise the metastore with `./create-uhrwerk-metastore.sh` (for also running the unit tests, execute `./create-uhrwerk-metastore-unit-tests.sh` and `./uhrwerk-example/load-yelp-data.sh <path-to-yelp_db.sql>`)

3. Now there are two ways how to run jobs:
    * From IntelliJ with local Spark. Use `testing-Connection-config.yml`, and `testing-env-config.yml`.
    ```scala
    val sparkSess = SparkSession
       .builder()
       .appName("LoaderD")
       .master("local[*]")
       .config("driver-memory", "2g")
       .config("spark.eventLog.enabled", "true")
       .config("spark.eventLog.dir", "./docker/spark_logs")
       .getOrCreate()
    ```
    * With the cli-tools.
      * For running inside the dockers:
      All configs need to be in the `docker/spark_configs/` folder.
      ```
      mvn package -DskipTests=true
      cp uhrwerk-cli/target/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar docker/spark_jars/
      cp uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT.jar docker/spark_jars/
      cp uhrwerk-common/src/main/resources/log4j.properties docker/spark_configs/
      ./uhrwerk-start.py staging.yelp_db.table_a_parq.1.0 /spark_jars/uhrwerk-example-0.1.0-SNAPSHOT.jar --table_configs /spark_configs/loader-A-parq-app.yml --conn_configs /spark_configs/testing-Connection-config-docker.yml --lower_bound 2012-05-01T00:00:00 --upper_bound 2012-05-06T00:00:00 --spark_docker --spark_properties /spark_configs/example_spark_docker.conf --uhrwerk_jar_location /spark_jars/uhrwerk-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar --uhrwerk_config /spark_configs/testing-env-config-docker.yml
      ```
    
4. When using the Spark docker containers, see the Spark history server at http://0.0.0.0:18080/
