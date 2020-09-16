#!/bin/bash

mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "DROP DATABASE UHRWERK_METASTORE"
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "DROP DATABASE UHRWERK_METASTORE_UNIT_TESTS"
