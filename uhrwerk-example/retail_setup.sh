#!/usr/bin/env bash
echo "-- building the docker image --"

docker build -t retail-mysql .

echo "-- running the docker image --"

docker run -d \
     --name retail_mysql \
     -e MYSQL_ROOT_PASSWORD=mysql \
     -p 43306:3306 \
     retail-mysql

sleep 5

echo "-- Building the Schema --"
docker exec -it retail_mysql bash -c "mysql < /db-data/oltpSchema.sql -u root -p"
echo "-- Creating the articifial data"
docker exec -it retail_mysql bash -c "mysql < /db-data/artificial_data.sql -u root -p"
echo "-- Setting foreign keys"
docker exec -it retail_mysql bash -c "mysql < /db-data/foreignKeys.sql -u root -p"