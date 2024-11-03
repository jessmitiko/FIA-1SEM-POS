import os
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("Template") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    df = spark.read.json('s3a://bronze/sample.json')
    df.show()
