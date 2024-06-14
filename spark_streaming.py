import logging
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from utils.utils import CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s'
)

logger = logging.getLogger("spark_structured_streaming")

def create_spark_session():
    try:
        spark = (
            SparkSession
            .builder
            .appName("SparkStructuredStreaming")
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
            .config("spark.cassandra.connection.port","9042")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully')
    except Exception:
        logging.error("Couldn't create the spark session")

    return spark


def create_initial_dataframe(spark_session):
    try:
        df = (
            spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", CONFIG.kafka['socket'])
            .option("subscribe", CONFIG.kafka['topic'])
            .option("delimeter",",")
            .option("startingOffsets", "earliest")
            .load()
        )
        df.show()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
    schema = T.StructType([
                T.StructField("full_name",T.StringType(),False),
                T.StructField("gender",T.StringType(),False),
                T.StructField("city",T.StringType(),False),
                T.StructField("country",T.StringType(),False),
                T.StructField("email",T.StringType(),False)
            ])

    df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(F.from_json(F.col("value"),schema).alias("data")).select("data.*")
    )

    return df

def start_streaming(df):
    logging.info("Streaming is being started...")

    my_query = (
        df.writeStream
        .format("org.apache.spark.sql.cassandra")
        .outputMode("append")
        .options(table=CONFIG.cassandra['table'], keyspace=CONFIG.cassandra['keyspace'])
        .option("checkpointLocation", "/tmp/rahul/checkpoint")
        .start()
    )

    return my_query.awaitTermination()
    

def main():
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    df_final = df_final.filter(F.col('full_name').isNotNull())
    start_streaming(df_final)

if __name__ == '__main__':
    main()