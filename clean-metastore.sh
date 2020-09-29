#!/bin/bash

echo "deleting all rows from all tables in the metastore"
mysql -u UHRWERK_USER -h 127.0.0.1 -P 53306 --password="Xq92vFqEKF7TB8H9" -e "delete from UHRWERK_METASTORE.CONNECTION;"
mysql -u UHRWERK_USER -h 127.0.0.1 -P 53306 --password="Xq92vFqEKF7TB8H9" -e "delete from UHRWERK_METASTORE.DEPENDENCY;"
mysql -u UHRWERK_USER -h 127.0.0.1 -P 53306 --password="Xq92vFqEKF7TB8H9" -e "delete from UHRWERK_METASTORE.PARTITION_;"
mysql -u UHRWERK_USER -h 127.0.0.1 -P 53306 --password="Xq92vFqEKF7TB8H9" -e "delete from UHRWERK_METASTORE.PARTITION_DEPENDENCY;"
mysql -u UHRWERK_USER -h 127.0.0.1 -P 53306 --password="Xq92vFqEKF7TB8H9" -e "delete from UHRWERK_METASTORE.SOURCE;"
mysql -u UHRWERK_USER -h 127.0.0.1 -P 53306 --password="Xq92vFqEKF7TB8H9" -e "delete from UHRWERK_METASTORE.TABLE_;"
mysql -u UHRWERK_USER -h 127.0.0.1 -P 53306 --password="Xq92vFqEKF7TB8H9" -e "delete from UHRWERK_METASTORE.TARGET;"
