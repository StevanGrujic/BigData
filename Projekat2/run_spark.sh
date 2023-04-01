#!/bin/bash

cd spark_app
docker build --rm -t bde/spark-app .
docker run --name projekat2 --net projekat2_default -p 4040:4040 -d bde/spark-app
