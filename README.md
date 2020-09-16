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
