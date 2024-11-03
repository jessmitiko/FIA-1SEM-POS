import os
import json
import boto3
import requests
from datetime import datetime

base_url='https://api.olhovivo.sptrans.com.br/v2.1'
token='bde4a3deabdd81f9206e0a69974a1a8eb18425137adbadf8e9cccf9d45ba53bd' #

s3_client=boto3.client(
    's3',
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    config=boto3.session.Config(signature_version='s3v4')
)

def save_to_s3(path_s3, data):
    bucket_name, key = path_s3.replace("s3a://", "").split('/', 1)
    current_timestamp=datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    print(f'saving {len(data)} items to {bucket_name} bucket in {key}dt_ingestion={current_timestamp}/data.json')
    s3_client.put_object(Body=(bytes(json.dumps(data, default=str).encode('UTF-8'))), Bucket=bucket_name, Key=f'{key}dt_ingestion={current_timestamp}/data.json')

    print('done!')

if __name__ == "__main__":
    response=requests.post(f'{base_url}/Login/Autenticar?token={token}')
    headers={"Cookie": response.headers['Set-Cookie']}

    response=requests.get(f'{base_url}/Linha/Buscar?termosBusca=TERM', headers=headers) #get only linhas serving terminais

    path_s3='s3a://bronze/linhas/'
    save_to_s3(path_s3, response.json())