#!/bin/bash

sed -e 's/UHRWERK_METASTORE/UHRWERK_METASTORE_UNIT_TESTS/g' uhrwerk-backend-mysql/src/main/resources/metastore_ddl/create_metastore_db_mysql.sql >uhrwerk-backend-mysql/src/main/resources/metastore_ddl/create_metastore_db_mysql_unit_tests.sql
sed -e 's/UHRWERK_METASTORE/UHRWERK_METASTORE_UNIT_TESTS/g' uhrwerk-backend-mysql/src/main/resources/metastore_ddl/metastore_ddl_mysql.sql >uhrwerk-backend-mysql/src/main/resources/metastore_ddl/metastore_ddl_mysql_unit_tests.sql

mysql -u root -h 127.0.0.1 -P 53306 --password="61ePGqq20u9TZjbNhf0" <uhrwerk-backend-mysql/src/main/resources/metastore_ddl/create_metastore_db_mysql_unit_tests.sql
mysql -u root -h 127.0.0.1 -P 53306 --password="61ePGqq20u9TZjbNhf0" <uhrwerk-backend-mysql/src/main/resources/metastore_ddl/metastore_ddl_mysql_unit_tests.sql
