import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("ingestion-linhas") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    today=datetime.now().strftime('%Y-%m-%d')
    df=spark.read.json(f's3a://bronze/linhas/dt_ingestion={today}*')

    df=df.withColumn('date', lit(today)) #add date as ingestion date

    df.show(5)

    # df.write.format('delta') \
    #     .mode('overwrite') \
    #     .option('overwriteSchema', 'true') \
    #     .partitionBy('date') \
    #     .save('s3a://silver/linhas/')

    df.write.option('header', 'true').csv('s3a://silver/linhas/data.csv')

    print('done!')
