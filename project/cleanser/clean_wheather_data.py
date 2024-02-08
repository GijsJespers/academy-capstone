from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf

import pyspark.sql.functions as F

def clean(frame: DataFrame) -> DataFrame:
    clean_frame = (frame
        .withColumn("lat", F.col("coordinates.latitude"))
        .withColumn("lon", F.col("coordinates.longitude"))
        .withColumn("date_local", F.col("date.local").cast("timestamp"))
        .withColumn("date_utc", F.col("date.utc").cast("timestamp"))
        .drop("date","coordinates")
    )
    return clean_frame

if __name__ == "__main__":
    config = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.0",
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }

    conf = SparkConf().setAll(config.items())
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Extract
    df = spark.read.json("s3a://dataminded-academy-capstone-resources/raw/open_aq/")
    
    clean_df = clean(df)
    clean_df.show()

