"""Vault helpers."""
import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Mapping
from functools import cache
import aiohttp
import zipfile
from core.ecs_vault_helper import create_ecs_client, download_file, get_vault_token

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Vault environment variables
role_id = os.environ.get('VAULT_ROLE_ID')
secret_id = os.environ.get('VAULT_SECRET_ID')
vault_addr = os.environ.get('VAULT_ADDR')
namespace = os.environ.get('VAULT_NAMESPACE')
kob_namespace = os.environ.get('KOB_NAMESPACE')
env = os.environ.get('ENVIRONMENT_TAG')
# ECS environment variables
ECS_ENDPOINT = os.environ.get('ECS_ENDPOINT')
ACCESS_KEY_ID = os.environ.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.environ.get('SECRET_ACCESS_KEY')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
PREFIX = os.environ.get('ENVIRONMENT_TAG')


def _escape(val: str) -> str:
    # Keep one line per entry by escaping literal newlines
    return (val or "").replace("\n", "\\n")


    
def download_and_extract_tokenizer(client):
    """Download tokenizer.zip (prod) or tokenizer_nonprod.zip (non-prod) from core bucket/prefix /tokenizers and extract it"""
    bucket_name = "core"
    prefix = "tokenizers/"
    
    # Choose file based on environment
    if env == "prod":
        object_key = prefix + "tokenizers.zip"
        local_filename = "tokenizers.zip"
    else:
        object_key = prefix + "tokenizers_nonprod.zip"
        local_filename = "tokenizers_nonprod.zip"
    
    local_zip_dir = "./deployment/tmp"
    extract_dir = "./core"

    logger.info(f"Current directory: {os.getcwd()}")
    # Ensure directories exist
    os.makedirs(local_zip_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    local_zip_path = os.path.join(local_zip_dir, local_filename)

    # Download the zip file
    if download_file(client, bucket_name, object_key, local_zip_path):
        # Extract contents
        logger.info(f"Extracting {local_zip_path} into {extract_dir}...")
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        logger.info("Extraction complete.")


@cache
async def read_secret(token: str, secret_path: str) -> dict:
    """Detect KV version by the presence of "/data/".Returns the dict with the secret data or None on error."""
    headers = {'X-Vault-Token': token}
    url = f"{vault_addr.rstrip('/')}/v1/{namespace.rstrip('/')}/kv/data/{secret_path}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session, session.get(
            url,
            headers=headers,
            ssl=False,
        ) as resp:
            return secret_path, await resp.json()
    except Exception:
        logger.exception('KV read failed:')
        return secret_path, {} 

def _flatten(
    data: Mapping[str, Any],
    parent_key: str = "",
    sep: str = "_",
) -> Dict[str, str]:
    """
    Recursively flatten a nested mapping into a flat ``{KEY: VALUE}`` dict
    where keys are upper‑cased and separated by ``sep``.
    Non‑string values are JSON‑encoded so they survive round‑tripping.
    """
    items: Dict[str, str] = {}
    for k, v in data.items():
        new_key = k
        if isinstance(v, Mapping):
            items.update(_flatten(v, new_key, sep=sep))
        elif isinstance(v, (list, tuple)):
            # Store list/tuple as JSON string – most .env parsers can handle it
            items[new_key.upper()] = json.dumps(v)
        else:
            # Cast primitive types to string
            items[new_key.upper()] = str(v)
    return items

async def download_secret(env):
    
    token = await get_vault_token(
            vault_addr=vault_addr,
            role_id=role_id,
            secret_id=secret_id,
            namespace=namespace,
        )

    # logger.info(f"Vault token retrieved: {token}")
    mapping = [f"CONSULTIQ/{env}/{i}" for i in ["DELL_ATTACHMENTS", "DATABASE", "GENAI", "KAFKA", "SERVICE"]]
    mapping.append(f"KOB/VAULT_CONFIG/{kob_namespace}")
    tasks = [asyncio.create_task(read_secret(token, mapping)) for mapping in mapping]
    configs = await asyncio.gather(*tasks) 
    config = {} 
    for i in configs:
        if "VAULT_CONFIG" in i[0]:
            config["SASL_PLAIN_PASSWORD"] = i[1]["data"]["data"]["PASSWORD"]
            config["SASL_PLAIN_USERNAME"] = i[1]["data"]["data"]["USERNAME"]
        else:
            config = config | _flatten(i[1]["data"]["data"])
    return config

REQUIRED_KEYS = {
    "ORACLE_DSN",
    "ORACLE_USER",
    "ORACLE_PASSWORD",
    "SVC_ACCOUNT_EMAIL",
    "SVC_ACCOUNT_PASSWORD_EMAIL",
    "LIBREOFFICE_PATH"
}

def download_env_from_s3(client: str, env: str, local_path: str = "./downloaded.env") -> dict:
    """
    Downloads .env for the given environment from:
    s3://config/<env>/.env
    Returns a dict of ONLY the required keys.
    """

    bucket_name = "config"
    object_key = f"{env.lower()}/.env"   # dev/.env, perf/.env , prod/.env

    logger.info(f"Downloading {object_key} from {bucket_name}")
    download_file(client, bucket_name, object_key, local_path)
    logger.info("Download successful")

    extracted = {}

    # Read and filter required values
    with open(local_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                key, value = line.split("=", 1)
                if key in REQUIRED_KEYS:
                    extracted[key] = value

    return extracted

if __name__ == "__main__":
    
    ecs_client = create_ecs_client(
        endpoint_url=ECS_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
        region_name="us-east-1",
        verify=False,
    )    

    # 1) Get existing secrets
    config = asyncio.run(download_secret(env.upper()))

    # 2) Download and extract ONLY required keys from S3 env file
    s3_required = download_env_from_s3(ecs_client, env)

    # 3) Merge: let S3 values override config if same keys appear
    merged = dict(config)
    merged.update(s3_required)

    # 4) Write out .env
    with Path("./.env").open("w", encoding="utf-8") as f:
        for key in merged.keys():
            # Escape any newline characters to keep the file single‑line per entry
            safe_val = _escape(merged.get(key, "--------------"))
            f.write(f"{key}={safe_val}\n")
        # Append your static values
        f.write(f"ECS_ENDPOINT={_escape(ECS_ENDPOINT)}\n")
        f.write(f"ACCESS_KEY_ID={_escape(ACCESS_KEY_ID)}\n")
        f.write(f"SECRET_ACCESS_KEY={_escape(SECRET_ACCESS_KEY)}\n")

    # download tokenizer

    download_and_extract_tokenizer(ecs_client)