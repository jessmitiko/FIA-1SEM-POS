import os
from datetime import datetime
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("final-linhas") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    today=datetime.now().strftime('%Y-%m-%d')

    posicao=spark.read.option('header', 'true').csv(f's3a://silver/posicao_by_linha/csv/')
    paradas=spark.read.option('header', 'true').csv(f's3a://silver/paradas_by_linhas/csv/')
    linhas=spark.read.option('header', 'true').csv(f's3a://silver/linhas/csv/')

    paradas=(paradas
        .withColumnRenamed('py', 'latitude_parada')
        .withColumnRenamed('px', 'longitude_parada')) #px and py exists in posicao

    df=(posicao
        .join(paradas, on=['cl', 'date'], how='left')
        .join(linhas, on=['cl', 'date'], how='left'))
    df.dropDuplicates(['cl', 'date'])

    with_columns_renamed=(df
        .withColumnRenamed('cl','codigo_linha')
        .withColumnRenamed('hr', 'hora_extracao_api')
        .withColumnRenamed('p', 'prefixo_veiculo')
        .withColumnRenamed('a', 'acessibilidade')
        .withColumnRenamed('ta', 'loc_capturada_hora')
        .withColumnRenamed('py', 'latitude')
        .withColumnRenamed('px', 'longitude')
        .withColumnRenamed('lc', 'circular')
        .withColumnRenamed('lt', 'prefixo_numero')
        .withColumnRenamed('sl', 'sentido')
        .withColumnRenamed('tp', 'letreiro_term_principal')
        .withColumnRenamed('ts', 'letreiro_term_secundario')
        .withColumnRenamed('cp', 'codigo_parada')
        .withColumnRenamed('np', 'nome_parada')
        .withColumnRenamed('ed', 'endereco_parada')
        .withColumnRenamed('tl', 'sufixo_numero')
    )

    with_columns_renamed.show(5)

    # df.write.format('delta') \
    #     .mode('overwrite') \
    #     .option('overwriteSchema', 'true') \
    #     .partitionBy('date') \
    #     .save('s3a://silver/linhas/')

    with_columns_renamed.write.option('header', 'true').mode('overwrite').csv('s3a://gold/linhas/csv/')

    print('done!')
