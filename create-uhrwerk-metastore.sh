#!/bin/bash

mysql -u root -h 127.0.0.1 -P 53306 --password="61ePGqq20u9TZjbNhf0" <uhrwerk-backend-mysql/src/main/resources/create_metastore_db_mysql.sql
mysql -u root -h 127.0.0.1 -P 53306 --password="61ePGqq20u9TZjbNhf0" <uhrwerk-backend-mysql/src/main/resources/metastore_ddl_mysql.sql
