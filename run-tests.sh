$!/bin/bash

docker run --name docker-mysql-uhrwerk-test -e MYSQL_ROOT_PASSWORD=mysql -p 43342:3306 -d mysql:8.0.21 ; docker kill docker-mysql ; docker rm docker-mysql
mvn test