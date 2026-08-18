import os
import json
from pyspark.sql.functions import struct, to_json
from pyspark.sql import SparkSession


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .master("local[2]") \
            .config("spark.driver.extraJavaOptions", "-Dlog4j2.configurationFile=file:log4j2.properties") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()

def save_tojson(df, filename):
    records = df.select(to_json(struct("*")).alias("value")).collect()
    op_dir = "test_data/results"
    os.makedirs(op_dir, exist_ok=True)

    path = os.path.join(op_dir, filename + ".json")
    with open(path, "w") as f:
        for row in records:
            f.write(row.value +"\n")