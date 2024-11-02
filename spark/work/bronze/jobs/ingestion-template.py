from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark=(SparkSession.builder
        .appName("JobTest")
        .enableHiveSupport()
        .getOrCreate())
    
    df=spark.read.json('s3a://bronze/sample.json')
    df.show()
