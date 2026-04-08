import os, logging
from dotenv import load_dotenv
import uuid, requests, zipfile, io, certifi, httpx
# from openai import OpenAI

import core.genai.authentication_provider as authentication_provider

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False

load_dotenv(r'.env', override=True)
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
# print(os.getenv("USE_SSO"))
use_sso = os.getenv("USE_SSO") == "true"
server_side_token_refersh = os.getenv("ENABLE_TOKEN_REFRESH_AT_SERVER_SIDE") == "true"

def validate_client_credentials():
    # if client_id == 'Insert_your_client_id_here' or client_id is None or client_secret == 'Insert_your_client_secret_here' or client_secret is None:
    #     print("*** Please set the CLIENT_ID & CLIENT_SECRET in environment variables or set Use_SSO to true. ***")
    #     raise Exception("Invalid client credentials")
    # else:
    #     print("Using Client Credentials")
    return 0

def get_correlation_id():
    return str(uuid.uuid4())

def update_certifi():
    try:
        return
        # URL to download the Dell certificates zip file
        url = "https://pki.dell.com//Dell%20Technologies%20PKI%202018%20B64_PEM.zip"
        # logger.info(f"Downloading Dell certificates zip from:{url}")
        response = requests.get(url)
        # Use raise_for_status() for concise error checking
        response.raise_for_status()
        # logger.info(f"Downloaded certificate zip, size:{len(response.content)} bytes")

        # Determine the location of the certifi bundle
        cert_path = certifi.where()
        # logger.info("Certifi bundle path:{cert_path}")

        # Define the names of the certificates within the zip file
        dell_root_cert_name = "Dell Technologies Root Certificate Authority 2018.pem"
        dell_issuing_cert_name = "Dell Technologies Issuing CA 101_new.pem"

        # Append the certificates directly from the zip archive in memory.
        # logger.info("Appending Dell certificates to certifi bundle...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Read certificate contents directly from the zip file in memory
            # Ensure decoding from bytes to string (assuming UTF-8)
            root_cert_content = z.read(dell_root_cert_name).decode('utf-8')
            issuing_cert_content = z.read(dell_issuing_cert_name).decode('utf-8')

            # Append the certificates to the certifi bundle
            # (Make sure you have backup of certifi bundle if needed.)
            with open(cert_path, "a") as bundle:
                bundle.write("\n")
                bundle.write(root_cert_content)
                bundle.write("\n") # Ensure newline after first cert
                bundle.write(issuing_cert_content)
                bundle.write("\n") # Ensure newline after second cert

        # logger.info("Dell certificates successfully added to certifi bundle.")

    except KeyError as e:
        # Handle case where expected certificate file is not in the zip
        logger.error(f"Error: Certificate file '{e}' not found in the zip archive.")
    except Exception as e:
        # Handle other potential errors during processing
        logger.error(f"An error occurred during certificate appending: {e}")

def get_default_headers_based_on_authentication(corrid: uuid):
    default_headers = {
            "x-correlation-id": corrid,
            'accept': '*/*',
            'Content-Type': 'application/json'
        }
    if use_sso:
        auth = authentication_provider.AuthenticationProvider()
        default_headers['Authorization'] = 'Bearer ' + auth.generate_auth_token()
    else:
        if server_side_token_refersh:
            auth = authentication_provider.AuthenticationProvider()
            default_headers['Authorization'] = 'Basic ' + auth.get_basic_credentials()

    return default_headers

def get_http_client_based_on_authentication():
    if use_sso:
        http_client=httpx.Client(verify=False)
    else:
        if server_side_token_refersh:
            http_client=httpx.Client(verify=False)
        else:
            auth = authentication_provider.AuthenticationProviderWithClientSideTokenRefresh()
            http_client=httpx.Client(auth=auth,verify=False)
    return http_client