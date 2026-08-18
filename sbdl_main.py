import sys
from lib import Utils, Configloader, Transformation
from lib.logger import Log4j2
from pyspark.sql.functions import col, struct, to_json

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    else:
        env_name = sys.argv[1].upper()
        load_date_in = sys.argv[2]
        spark = Utils.get_spark_session(env_name)
        log = Log4j2(spark)
        conf = Configloader.get_config(env_name)
        final_df = Transformation.transformations(spark, env_name)
        log.info("Preparing to send data to Kafka")
        kafka_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"), to_json(struct("*")).alias("value"))

        api_key = conf["kafka.api_key"]
        api_secret = conf["kafka.api_secret"]

        kafka_df.write.format("kafka") \
                    .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
                    .option("topic", conf["kafka.topic"]) \
                    .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
                    .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
                    .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
                    .option("kafka.ssl.truststore.type", conf["kafka.ssl.truststore.type"]) \
                    .option("kafka.ssl.truststore.location", conf["kafka.ssl.truststore.location"]) \
                    .save()

        log.info("Finished sending data to Kafka")
