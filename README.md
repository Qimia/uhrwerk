# Qimia Uhrwerk
A data-dependency manager, data-pipeline scheduler, and data-lake build tool.

## Modules

- Uhrwerk-common: Model Pojo's and module-interfaces
- Uhrwerk-backend-mysql: Database access objects which implement persistence- and dependency-interfaces
- Uhrwerk-config: Configuration by loading yaml files or by using java-builders
- Uhrwerk-framemanager: implementation of the load/read dataframe interface.
- Uhrwerk-engine: Management of Tables and methods to execute tasks
- Uhrwerk-example: Example usage of Uhrwerk

## Testing
For quick unit tests run `mvn -DtagsToExclude=io.qimia.uhrwerk.tags.DbTest,io.qimia.uhrwerk.tags.Slow test` to disable Mysql dependent and slower tests.

## Setup

Uhrwerk needs two things to run:
  * A metastore running on a mysql docker container.
  * A Spark installation (either locally or using docker).
  

1. Start docker-compose
  
    1.1 For a local Spark installation: Run `./start-docker.sh metastore-only` to start up docker-compose with only the metastore mysql container and an adminer. 
    
    1.2 With no local Spark installation: 
    * Run `./build-spark-docker-images.sh` to build the necessary Spark docker images. This takes a while.
    * Run `./start-docker.sh all` to start up docker-compose with both the metastore and spark containers (by default one master and one worker).

2. (Only necessary when running for the first time): Initialise the metastore with `./create-uhrwerk-metastore.sh` (for also running the unit tests, execute `./create-uhrwerk-metastore-unit-tests.sh` and `./uhrwerk-example/load-yelp-data.sh <path-to-yelp_db.sql>`)

3. Now there are two ways how to run jobs:
    * From IntelliJ with local Spark. Use `testing-connection-config.yml`, and `testing-env-config.yml`.
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
    * With prepared spark-submit scripts on the docker Spark cluster (`./submit-loader-A-parq.sh`). Use `testing-connection-config-docker.yml`, and `testing-env-config-docker.yml`.