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
