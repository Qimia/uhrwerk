#!/bin/bash

./start-docker.sh
mvn test
docker-compose -f uhrwerk-core/docker/docker-compose.yml down