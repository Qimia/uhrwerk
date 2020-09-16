#!/bin/bash

mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" <uhrwerk-backend-mysql/src/main/resources/create_metastore_db_mysql.sql
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" <uhrwerk-backend-mysql/src/main/resources/metastore_ddl_mysql.sql
