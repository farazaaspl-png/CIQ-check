import boto3
import os
from pathlib import Path
from config import Configuration
from botocore.exceptions import ClientError
from botocore.config import Config
import shutil
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)

class StorageManager:
    """Manage S3/ECS operations including bucket listing, file uploads, and downloads"""
    cfg = Configuration()
    cfg.load_active_config()
    def __init__(self):
        self.endpoint_url = StorageManager.cfg.ECS_ENDPOINT
        self.access_key_id = StorageManager.cfg.ACCESS_KEY_ID
        self.secret_access_key = StorageManager.cfg.SECRET_ACCESS_KEY
        self.bucket_name = StorageManager.cfg.ECS_BUCKET
        self.region_name = StorageManager.cfg.ECS_REGION
        self.verify = StorageManager.cfg.ECS_VERIFY
        
        self.client = self._create_client()
    
    def _create_client(self, upload = False):
        """Create and return an S3 client configured for ECS"""
        s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=Config( 
                signature_version='s3' if upload else 's3v4',
                retries={"max_attempts": 1},
                s3={
                    "payload_signing_enabled": False,
                    "checksum_validation": False,
                    "addressing_style": "path",
                }
            ),
            region_name=self.region_name,
            verify=self.verify
        )
        return s3_client
    
    @staticmethod
    def _make_s3_key(local_path: str, strip_prefix: str | None = None) -> str:
        """
        Convert a local filesystem path to a clean S3 key.
        - Forward‑slashes only.
        - Optionally strip a leading directory (e.g. the project root) so the key is relative.
        """
        p = Path(local_path)

        if strip_prefix:
            # Normalise both sides before stripping
            strip_path = Path(strip_prefix).resolve()
            try:
                p = p.resolve().relative_to(strip_path)
            except ValueError:
                pass
        return p.as_posix()
    
    def exists(self, file_path, is_dir = False):
        s3_key = self._make_s3_key(file_path,StorageManager.cfg.DATA_DIR)
        logger.info(f"Checking if object exists: {s3_key}")
        client = self._create_client(upload=True)

        # Check if it's a directory (ends with / or we should treat as directory)
        if is_dir:
            self.get_list_of_object(s3_key)
            return len(self.get_list_of_object(s3_key))>0
        else:
            return self._file_exists(client, s3_key)
    
    def delete_file(self, s3_key):
        """
        Delete a file from ECS bucket.
        
        Args:
            s3_key: The S3 key of the file to delete
            
        Returns:
            bool: True if deletion successful, False otherwise
        """
        try:
            logger.info(f"Deleting file: s3://{self.bucket_name}/{s3_key}")
            client = self._create_client(upload=True)
            
            # Check if file exists before attempting deletion
            if not self._file_exists(client, s3_key):
                logger.warning(f"⚠️ File does not exist: s3://{self.bucket_name}/{s3_key}")
                return False
            
            client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"✅ Successfully deleted: s3://{self.bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Error deleting file: {e}")
            return False

    def delete_files(self, s3_key, file_ext = '.json'):
        files = self.get_list_of_object(s3_key)
        for file in files:
            if file.endswith(file_ext):
                self.delete_file(file)

    def _file_exists(self, client, s3_key):
        """Check if a file exists using head_object"""
        try:
            client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ["NoSuchKey", "404"]:
                return False
            else:
                logger.warning(f"❌ Error checking S3 file existence: {e}")
                return False

    def upload(self, file_path,overwrite=False):
        """Upload a file to S3/ECS"""
        
        if not os.path.isfile(file_path):
            logger.info(f"❌ Local file does not exist: {file_path}")
            return False
        
        s3_key = self._make_s3_key(file_path,StorageManager.cfg.DATA_DIR)
        client = self._create_client(upload=True)
        # Check if file exists on S3
        try:
            client.head_object(Bucket=self.bucket_name, Key=s3_key)
            file_exists = True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                file_exists = False
            else:
                logger.error(f"❌ Error checking S3 file existence: {e}")
                raise e
        
        if file_exists and not overwrite:
            logger.info(f"⚠️ File already exists on S3 and overwrite=False: s3://{self.bucket_name}/{s3_key}")
            return False
        
        try:
            logger.info(f"\nUploading {file_path} → s3://{self.bucket_name}/{s3_key} ...")
            client.upload_file(
                Filename=file_path,
                Bucket=self.bucket_name,
                Key=s3_key
            )
            logger.info("Upload successful")
            return True
        except ClientError as e:
            logger.error(f"❌ Error uploading file: {e}")
            return False
    
    def get_bucket_list(self):
        """List all available buckets"""
        try:
            response = self.client.list_buckets()
            return response['Buckets']
        except ClientError as e:
            logger.error(f"Error listing buckets: {e}")
            return []
    
    def get_list_of_object(self, s3_path=''):
        """List all objects in a bucket"""
        try:
            paginator = self.client.get_paginator("list_objects_v2")

            operation_parameters = { "Bucket": self.bucket_name}

            if s3_path:
                operation_parameters["Prefix"] = s3_path

            object_keys = []

            for page in paginator.paginate(**operation_parameters):
                for obj in page.get("Contents", []):
                    object_keys.append(obj["Key"])
            return object_keys
        except ClientError as e:
            logger.error(f"Error listing objects: {e}")
            return []
    
    def download(self, s3_path):
        """Download a file from ECS to local storage"""
        try:
            
            localpath = os.path.join(StorageManager.cfg.DATA_DIR,s3_path)
            os.makedirs(os.path.dirname(localpath), exist_ok=True)
            logger.info(f"\nDownloading {s3_path} to {localpath}...")
            self.client.download_file(self.bucket_name, s3_path, localpath)
            logger.info(f"Successfully downloaded {s3_path}")
            return localpath
        except ClientError as e:
            logger.error(f"Error downloading file: {e}")
            raise e
    
    def download_all(self, s3_dir):
        """Download all files from a bucket"""
        # Create local directory if it doesn't exist
        os.makedirs(StorageManager.cfg.DATA_DIR, exist_ok=True)
        objects = self.get_list_of_object(s3_dir) 
        for s3_file_path in objects:
            self.download(s3_file_path)
        return os.path.join(StorageManager.cfg.DATA_DIR,s3_dir)
    
    def read_file_content(self, s3_file_path):
        """Read file content directly into memory"""
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=s3_file_path)
            content = response['Body'].read()
            logger.info(f"\nContent of {s3_file_path}:")
            logger.info(content.decode('utf-8'))
            return content
        except ClientError as e:
            logger.error(f"Error reading file: {e}")
            return None
        
    def upload_file(self, local_file_path, object_key=None, overwrite=False, delete_after_upload=False):
        """
        Upload a single file to ECS bucket using put_object (recommended for ECS).
    
        Args:
            client: Boto3 S3 client
            bucket_name: Target ECS/S3 bucket
            local_file_path: Path to local file to upload
            object_key: (optional) Object key to use in S3 (defaults to filename)
            overwrite: (optional) Whether to overwrite existing file (default True)
        """

        if not os.path.isfile(local_file_path):
            logger.info(f"❌ Local file does not exist: {local_file_path}")
            return False
        
        client = self._create_client(upload=True)
        if not overwrite:
        # Check if file exists on S3
            try:
                client.head_object(Bucket=self.bucket_name, Key=object_key)
                file_exists = True
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    file_exists = False
                else:
                    logger.error(f"❌ Error checking S3 file existence: {e}")
                    raise e
            
            if file_exists:
                logger.info(f"⚠️ File already exists on S3 and overwrite=False: s3://{self.bucket_name}/{object_key}")
                return False
        
        try:
            logger.info(f"\nUploading {local_file_path} → s3://{self.bucket_name}/{object_key} ...")
            client.upload_file(
                Filename=local_file_path,
                Bucket=self.bucket_name,
                Key=object_key
            )
            logger.info("Upload successful")
            if delete_after_upload:
                os.remove(local_file_path)
                logger.info(f"Deleted local file: {local_file_path}")
            return True
        except ClientError as e:
            logger.error(f"❌ Error uploading file: {e}")
            return False

    # Upload all files in a folder
    def upload_folder(self, folder_path, prefix='', delete_after_upload=False):
        """Upload all files from a local folder (recursively)"""
        try:
            for root, _, files in os.walk(folder_path):
                for file_name in files:
                    local_path = os.path.join(root, file_name)
                    # Construct object key with folder structure
                    relative_path = os.path.relpath(local_path, folder_path)
                    object_key = os.path.join(prefix, relative_path).replace("\\", "/")
    
                    self.upload_file(local_path, object_key, overwrite=True, delete_after_upload=delete_after_upload)
            print(f"✅ All files from '{folder_path}' uploaded successfully.")
            
            if delete_after_upload:
                # Delete empty folders after files have been removed
                for item in os.listdir(folder_path):
                    item_path = os.path.join(folder_path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, ignore_errors=True)
                print(f"Deleted folder: {folder_path}")
        except Exception as e:
            print(f"❌ Error uploading folder: {e}")
