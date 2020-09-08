#!/bin/bash

./start-docker.sh
mvn test
./stop-docker.sh
