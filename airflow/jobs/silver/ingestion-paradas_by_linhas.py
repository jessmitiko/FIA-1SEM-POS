import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, explode

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
        .getOrCreate()
    
    today=datetime.now().strftime('%Y-%m-%d')
    df=spark.read.json(f's3a://bronze/paradas_by_linha/dt_ingestion={today}*')

    #plan struct df to save to a csv
    exploded_df=df.withColumn("parada", explode(df.paradas))
    flat_df=exploded_df.select(
        df.cl,
        exploded_df.parada.cp.alias("cp"),
        exploded_df.parada.np.alias("np"),
        exploded_df.parada.ed.alias("ed"),
        exploded_df.parada.py.alias("py"),
        exploded_df.parada.px.alias("px")
    )

    flat_df=flat_df.withColumn('date', lit(today)) #add date as ingestion date

    flat_df.show(5)

    # df.write.format('delta') \
    #     .mode('overwrite') \
    #     .option('overwriteSchema', 'true') \
    #     .partitionBy('date') \
    #     .save('s3a://silver/linhas/')

    flat_df.write.option('header', 'true').mode('overwrite').csv('s3a://silver/paradas_by_linhas/csv/')

    print('done!')
