import os

from functools import partial
from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    mysql_host = os.environ.get("MYSQL_HOST", "127.0.0.1")
    mysql_db = os.environ.get("MYSQL_DATABASE", "db")
    mysql_user = os.environ.get("MYSQL_USER", "root")
    mysql_pass = os.environ.get("MYSQL_PASSWORD","pass")

    spark.read.format("jdbc").options(
        url ="jdbc:mysql://{}/{}".format(mysql_host, mysql_db),
        driver="com.mysql.jdbc.Driver",
        dbtable="rules_results",
        user=mysql_user,
        password=mysql_pass).load().show()
