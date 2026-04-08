# ecs_utils.py
import logging
from typing import Optional
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import aiohttp
from functools import cache
import os

logger = logging.getLogger(__name__)

# ---------------- ECS Helpers ----------------
def create_ecs_client(
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    region_name: str = "us-east-1",
    verify: bool = False,
    upload: bool = False,
):
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(signature_version='s3' if upload else 's3v4',
                      s3={
                        "addressing_style": "path",
                        "use_unsigned_payload": True,
                        "payload_signing_enabled": False
                      }
                    ),
        region_name=region_name,
        verify=verify,
    )
    return s3_client


def download_file(client, bucket_name: str, object_key: str, local_path: str) -> bool:
    try:
        logger.info(f"Downloading s3://{bucket_name}/{object_key} -> {local_path} ...")
        client.download_file(bucket_name, object_key, local_path)
        logger.info(f"Successfully downloaded {object_key}")
        return True
    except ClientError as e:
        logger.error(f"Error downloading file {object_key}: {e}")
        return False
    
# ---------------- Vault Helpers ----------------
@cache
async def get_vault_token(
    vault_addr: str,
    role_id: str,
    secret_id: str,
    namespace: Optional[str] = None,
) -> Optional[str]:

    """Get Vault token."""
    
    if not (role_id and secret_id):
        logger.error('❌ Role ID and Secret ID are required for AppRole auth.')
        return None

    login_url = f'{vault_addr.rstrip('/')}/v1/auth/approle/login'
    payload = {'role_id': role_id, 'secret_id': secret_id}
    namespace_header = {'X-Vault-Namespace': namespace}

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session, session.post(
            login_url,
            json=payload,
            headers=namespace_header,
            ssl=False,
        ) as resp:
            data = await resp.json()
            return data['auth']['client_token']
    except Exception:
        logger.exception('AppRole login failed:')
        return None
    
# -------------------------------------------------File Uploader-----------------------------------------------------

def upload_file(client, bucket_name, local_file_path, object_key=None, overwrite=True):
    """
    Upload a single file to ECS bucket using put_object (recommended for ECS).
 
    Args:
        client: Boto3 S3 client
        bucket_name: Target ECS/S3 bucket
        local_file_path: Path to local file to upload
        object_key: (optional) Object key to use in S3 (defaults to filename)
        overwrite: (optional) Whether to overwrite existing file (default True)
    """
    try:
        if not os.path.isfile(local_file_path):
            print(f"❌ File not found: {local_file_path}")
            return False
 
        if not object_key:
            object_key = os.path.basename(local_file_path)
 
        # Check if object already exists (if overwrite=False)
        if not overwrite:
            try:
                client.head_object(Bucket=bucket_name, Key=object_key)
                print(f"⚠️ Object '{object_key}' already exists in '{bucket_name}', skipping upload.")
                return False
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise  # Unexpected error
 
        print(f"📤 Uploading '{local_file_path}' to bucket '{bucket_name}' as '{object_key}'...")
        with open(local_file_path, "rb") as f:
            client.put_object(Bucket=bucket_name, Key=object_key, Body=f)
 
        print(f"✅ Upload successful: {object_key}")
        return True
 
    except ClientError as e:
        print(f"❌ ClientError while uploading: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
 
 
# Upload all files in a folder
def upload_folder(client, bucket_name, folder_path, prefix=''):
    """Upload all files from a local folder (recursively)"""
    try:
        for root, _, files in os.walk(folder_path):
            for file_name in files:
                local_path = os.path.join(root, file_name)
                # Construct object key with folder structure
                relative_path = os.path.relpath(local_path, folder_path)
                object_key = os.path.join(prefix, relative_path).replace("\\", "/")
 
                upload_file(client, bucket_name, local_path, object_key)
        print(f"✅ All files from '{folder_path}' uploaded successfully.")
    except Exception as e:
        print(f"❌ Error uploading folder: {e}")