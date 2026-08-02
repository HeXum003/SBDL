from pyspark.sql import SparkSession


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .master("local[2]") \
            .enableHiveSupport() \
            .config("spark.driver.extraJavaOptions", "-Dlog4j2.configurationFile=file:log4j2.properties") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()
