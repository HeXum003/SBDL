import configparser
from pyspark import SparkConf

def get_config(env):
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}
    for key, value in config.items(env):
        conf[key] = value
    return conf

def get_spark_config(env):
    sparkconf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for key, value in config.items(env):
        sparkconf.set(key,value)

    return sparkconf

def get_data_filter(env, data_filter):
    config = get_config(env)
    return True if config[data_filter] == "" else config[data_filter]