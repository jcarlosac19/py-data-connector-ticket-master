import io
import uuid
from google.cloud import storage
from datetime import datetime
import hashlib
import base64
from pandas import DataFrame
import logging

logger = logging.getLogger("GStorage")

def compute_md5_bytes(data: bytes) -> str:
    """
    Compute base64-encoded MD5 hash (GCS format) for the given bytes.
    """
    md5 = hashlib.md5(data).digest()
    return base64.b64encode(md5).decode('utf-8')

def does_hash_exist(bucket, base_path: str, hash_to_check: str) -> bool:
    """
    Check if any blob under base_path has the same MD5 hash.
    """
    for blob in bucket.list_blobs(prefix=base_path):
        if blob.md5_hash == hash_to_check:
            logger.info(f"Matching file found: {blob.name}")
            return True
    return False

def upload_dataframe_to_gcs(df: DataFrame, bucket_name: str, base_path: str) -> str:
    """
    Uploads a Pandas DataFrame to GCS in Parquet format without saving locally.

    :param df: Pandas DataFrame to upload
    :param bucket_name: Name of the GCS bucket
    :param base_path: Base path within the bucket to store the file
    :return: GCS path where the file was saved
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    dt = datetime.now()

    # Generate a unique filename using a timestamp + UUID
    unique_id = uuid.uuid4().hex[:8]  # Shortened UUID for readability
    file_name = f"data_{dt.strftime('%Y%m%d_%H%M%S')}_{unique_id}.parquet"

    gcs_path = (
        f"{base_path}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/{file_name}"
    )

    # Using with context for better resource management
    with io.BytesIO() as buffer:
        df.to_parquet(buffer, engine="pyarrow")

        # Get the bytes to hash
        data_bytes = buffer.getvalue()

        # Compute hash
        data_md5 = compute_md5_bytes(data_bytes)

        if does_hash_exist(bucket, base_path, data_md5):
            logger.info("A file with identical content already exists. Skipping upload.")
            return ""

        buffer.seek(0)

        blob = bucket.blob(gcs_path)
        blob.upload_from_file(buffer, content_type="application/octet-stream")

    return gcs_path