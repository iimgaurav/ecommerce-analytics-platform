"""
setup_minio.py
Creates all required MinIO buckets if they don't exist.
Usage: python -m ingestion.setup_minio
"""

import os
import boto3
from botocore.client import Config

REQUIRED_BUCKETS = ["bronze", "silver", "gold"]


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def create_buckets():
    client = get_minio_client()
    print("\nSetting up MinIO buckets...")

    for bucket in REQUIRED_BUCKETS:
        try:
            client.head_bucket(Bucket=bucket)
            print(f"  ✅ '{bucket}' already exists")
        except Exception:
            client.create_bucket(Bucket=bucket)
            print(f"  🆕 '{bucket}' created!")

    print("\n✅ All buckets ready!\n")


if __name__ == "__main__":
    create_buckets()
