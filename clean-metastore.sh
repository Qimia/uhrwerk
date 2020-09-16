#!/bin/bash

echo "deleting all rows from all tables in the metastore"
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "delete from UHRWERK_METASTORE.CONNECTION;"
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "delete from UHRWERK_METASTORE.DEPENDENCY;"
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "delete from UHRWERK_METASTORE.PARTITION_;"
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "delete from UHRWERK_METASTORE.PARTITION_DEPENDENCY;"
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "delete from UHRWERK_METASTORE.SOURCE;"
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "delete from UHRWERK_METASTORE.TABLE_;"
mysql -u root -h 127.0.0.1 -P 53306 --password="mysql" -e "delete from UHRWERK_METASTORE.TARGET;"
