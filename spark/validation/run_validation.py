import os

from functools import partial
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

mysql_host = os.environ.get("MYSQL_HOST", "127.0.0.1")
mysql_db = os.environ.get("MYSQL_DATABASE", "db")
mysql_user = os.environ.get("MYSQL_USER", "root")
mysql_pass = os.environ.get("MYSQL_PASSWORD","pass")
mysql_url ="jdbc:mysql://{mysql_host}/{mysql_db}".format(mysql_host=mysql_host, mysql_db=mysql_db)

def get_stores():
    return spark.read.format("jdbc").options(
        url=mysql_url,
        driver="com.mysql.jdbc.Driver",
        dbtable="stores",
        user=mysql_user,
        password=mysql_pass).load()

def get_rules():
    return spark.read.format("jdbc").options(
        url=mysql_url,
        driver="com.mysql.jdbc.Driver",
        dbtable="rules",
        user=mysql_user,
        password=mysql_pass).load()

def save_validation_results(df):
    df.write.mode("overwrite").format("jdbc").options(
        url=mysql_url,
        driver="com.mysql.jdbc.Driver",
        dbtable="rules_results",
        user=mysql_user,
        password=mysql_pass).save()


def construct_rules(rules_data_frame):
    r2c = {}
    rules_raw = df_rules.rdd.map(lambda x: (x.rule_name, x.rule_dsl)).collectAsMap()
    for k, v in rules_raw.items():
        exec("r2c['{k}'] = lambda val: {v}".format(k=k, v=v))
    return r2c


def apply_rules(rule_to_column_mapping, columns, row):
    vr = []
    for column in columns:
        validator_func = "validators_{column}".format(column=column)
        if  validator_func in rule_to_column_mapping:
            result = rule_to_column_mapping[validator_func](row[column])
            res = {"id": row.id, "entity":"stores","attribute":column, "rule_name": validator_func, "rule_result": result}
            vr.append(res)
    return vr

def vresults_to_df(vresults):
    rows = map(lambda x: Row(**x), vresults)
    return spark.createDataFrame(rows)


if __name__ == "__main__":
    df_rules = get_rules()
    rule_to_column_mapping = construct_rules(df_rules)

    df_stores = get_stores()
    validation_results = []
    for row in df_stores.collect():
        vr = apply_rules(rule_to_column_mapping, df_stores.columns, row)
        validation_results = validation_results + vr

    df_results = vresults_to_df(validation_results)
    save_validation_results(df_results)
