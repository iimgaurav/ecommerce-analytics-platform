"""
setup_minio.py
Creates all required MinIO buckets if they don't exist.
Usage: python -m ingestion.setup_minio
"""

from ingestion.minio_client import get_minio_client

REQUIRED_BUCKETS = ["bronze", "silver", "gold"]


def create_buckets():
    client = get_minio_client()
    print("\nSetting up MinIO buckets...")
    for bucket in REQUIRED_BUCKETS:
        try:
            client.head_bucket(Bucket=bucket)
            print(f"  Already exists: {bucket}")
        except Exception:
            client.create_bucket(Bucket=bucket)
            print(f"  Created: {bucket}")
    print("\nAll buckets ready!\n")


if __name__ == "__main__":
    create_buckets()
