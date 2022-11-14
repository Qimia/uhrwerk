# Setup Yelp example

In this example we re-use the metastore-db as source and target DB/DWH.

But make sure to load the source YELP data into the source db
by using `./load-yelp-data.sh path/to/yelp_data/yelpdb.sql`.

Next make sure any logging set in the code & config works for docker/non-docker
(and make sure the directory exists). Plus the configuration set in 
`uhrwerk-example/src/main/resources/yelp_test/testing-Connection-config.yml` should
have the wanted datalake and datasource locations and DB Connection settings.

LoaderA and LoaderAParq are written in a way that uses Uhrwerk as a library.
These can run easily in Intellij with debugging. LoaderAParq relies of parquet dumps
from the Yelp-db (see tools/Download*).

The next test is to run it as a Framework but still inside of Intellij
(and still easy to debug) as AppLoaderAParq. Here we use the same UhrwerkAppRunner
that the CLI also uses.

The final test is to run it fully as framework using the python-cli wrapper.
It handles things like setting the spark config and using an existing Spark cluster.

`./uhrwerk-start.py staging.yelp_db.table_a_parq_app.1.0 ../uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT.jar --uhrwerk_config ../uhrwerk-example/src/main/resources/yelp_test/uhrwerk.yml --conn_configs ../uhrwerk-example/src/main/resources/yelp_test/testing-Connection-config.yml --lower_bound 2012-05-04T00:00:00 --upper_bound 2012-05-09T00:00:00 --table_configs ../uhrwerk-example/src/main/resources/yelp_test/staging/yelp_db/table_a_parq_app/table_a_parq_app_1.0.yml`

This command can be made a lot shorter because we use the config directory convention:

`./uhrwerk-start.py staging.yelp_db.table_a_parq_app.1.0 ../uhrwerk-example/target/uhrwerk-example-0.1.0-SNAPSHOT.jar --config_directory ../uhrwerk-example/src/main/resources/yelp_test/ --lower_bound 2012-05-01T00:00:00 --upper_bound 2012-05-03T00:00:00`