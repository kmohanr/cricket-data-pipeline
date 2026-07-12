import boto3

s3 = boto3.client("s3")

def upload(local_file, bucket, object_name):

    s3.upload_file(
        local_file,
        bucket,
        object_name
    )
