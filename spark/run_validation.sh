#!/bin/bash
$SPARK_HOME/bin/spark-submit \
--master local \
--packages mysql:mysql-connector-java:5.1.38 \
validation/run_validation.py
