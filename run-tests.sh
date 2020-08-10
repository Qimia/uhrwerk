#!/bin/bash

docker run --name docker-mysql-uhrwerk-test -e MYSQL_ROOT_PASSWORD=mysql -p 43342:3306 -d mysql:8.0.21
mvn test
docker kill docker-mysql-uhrwerk-test
docker rm docker-mysql-uhrwerk-test