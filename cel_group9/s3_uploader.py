import boto3
import logging
import requests
from io import BytesIO
from typing import Optional
import os
from urllib.parse import urlparse
from config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET,
    AWS_REGION, S3_IMAGES_PATH, TEMP_DIR
)
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Uploader:
    """Upload images and files to AWS S3"""
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        self.bucket_name = AWS_S3_BUCKET
        Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
    def _is_valid_url(self, url: str) -> bool:
        if not url:
            return False
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    def upload_image_from_url(self, image_url: str, filename: str, s3_path: str = S3_IMAGES_PATH) -> Optional[str]:
        if not image_url:
            logger.warning(f"Empty image URL for {filename}")
            return None
        if not self._is_valid_url(image_url):
            logger.error(f"Invalid image URL for {filename}: {image_url}")
            return None
        try:
            logger.info(f"Downloading image from {image_url}")
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()
            s3_key = f"{s3_path}/{filename}"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=response.content,
                ContentType='image/jpeg'
            )
            logger.info(f"Uploaded image to s3://{self.bucket_name}/{s3_key}")
            return s3_key
        except Exception as e:
            logger.error(f"Error uploading image {filename}: {str(e)}")
            return None
    def upload_local_file(self, local_path: str, s3_path: str, filename: str = None) -> Optional[str]:
        try:
            if not os.path.exists(local_path):
                logger.warning(f"Local file not found: {local_path}")
                return None
            if not filename:
                filename = os.path.basename(local_path)
            s3_key = f"{s3_path}/{filename}"
            logger.info(f"Uploading local file {local_path} to S3")
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                s3_key
            )
            logger.info(f"Uploaded file to s3://{self.bucket_name}/{s3_key}")
            return s3_key
        except Exception as e:
            logger.error(f"Error uploading file {local_path}: {str(e)}")
            return None
    def list_objects(self, s3_path: str) -> list:
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_path
            )
            return response.get('Contents', [])
        except Exception as e:
            logger.error(f"Error listing objects: {str(e)}")
            return []
    def get_s3_url(self, s3_key: str) -> str:
        return f"s3://{self.bucket_name}/{s3_key}"
    def generate_presigned_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            return None
    def test_connection(self) -> bool:
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info("S3 connection successful")
            return True
        except Exception as e:
            logger.error(f"S3 connection failed: {str(e)}")
            return False
