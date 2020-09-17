#!/usr/bin/env bash
echo "-- building the docker image --"

docker build -t retail-mysql .

echo "-- running the docker image --"

docker kill retail_mysql
docker rm retail_mysql
docker run -d \
  --name retail_mysql \
  -e MYSQL_ROOT_PASSWORD=mysql \
  -p 43306:3306 \
  retail-mysql

sleep 15

echo "-- Building the Schema --"
docker exec -it retail_mysql bash -c "mysql < /db-data/oltpSchema.sql -u root --password=mysql"
echo "-- Creating the artificial data"
docker exec -it retail_mysql bash -c "mysql < /db-data/artificial_data.sql -u root --password=mysql"
echo "-- Setting foreign keys"
docker exec -it retail_mysql bash -c "mysql < /db-data/foreignKeys.sql -u root --password=mysql"
