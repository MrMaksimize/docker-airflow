import os
import boto3
import subprocess

client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('CONN_S3_ACCESS_KEY'),
    aws_secret_access_key=os.environ.get('CONN_S3_SECRET'))

s3 = boto3.resource(
    's3',
    aws_access_key_id=os.environ.get('CONN_S3_ACCESS_KEY'),
    aws_secret_access_key=os.environ.get('CONN_S3_SECRET'))
prod = s3.Bucket('datasd-prod')

url = 'https://s3-us-west-1.amazonaws.com/datasd-prod/'

for key in prod.objects.all():
    key = key.key
    wget_url = url + key
    path = key.split('/')[-1]
    subprocess.call(['wget', '-O', './prod/' + path, wget_url])
