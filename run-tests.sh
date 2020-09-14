#!/bin/bash

./start-docker.sh

./create-uhrwerk-metastore-unit-tests.sh

mvn test

./stop-docker.sh
