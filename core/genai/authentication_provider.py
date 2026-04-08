import os
from aia_auth import auth
import base64
import httpx
import math
import time
import uuid
import requests
import zipfile
import io
import certifi

class AuthenticationProvider:
    def __init__(self):

        self.use_sso = os.getenv("USE_SSO").lower() == "true"
        # # Below properties are applicable to OAUTH only
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")

    def generate_auth_token(self):
        """
        Generates and returns an authentication token based on the configured method.

        The method defaults to `client_credentials` if `use_sso` is not explicitly
        set to "true".

        Returns:
            str: The generated authentication token.
        """
        if self.use_sso:
            return self._sso()
                
        return self._get_bearer_token()
    
    def get_basic_credentials(self):
        """
        Authenticates a request using either Single Sign-On or Client & Secret based on the value of USE_SSO.
        
        Parameters:
            request: The request object to authenticate.
        
        Returns:
            The authenticated request object.
        """
        self._validate_client_credentials()
        return base64.b64encode(f'{self.client_id}:{self.client_secret}'.encode()).decode()

    def _get_bearer_token(self):
        """
        Generates an authentication token using the Client Credentials flow.

        This method assumes that `client_id` and `client_secret` are globally
        available or passed in a different context. It first validates these
        credentials before requesting a token.

        Returns:
            str: The authentication token.
        """
        self._validate_client_credentials()
        return auth.client_credentials(self.client_id, self.client_secret).token

    def _sso(self):
        """
        Generates an authentication token using the Single Sign-On (SSO) flow.

        This method leverages the `auth.sso()` function to obtain a token,
        which typically involves a user interaction or a pre-configured
        session.

        Returns:
            str: The authentication token.
        """
        access_token = auth.sso()
        return access_token.token    
    
    def _validate_client_credentials(self):
        """
        Validates client credentials. Checks if client ID and client secret are set and not equal to default values.

        Parameters:
            self (AuthenticationProvider): The instance of the class that this function is a part of.

        Returns:
            None: If the client credentials are valid, the function does not return anything. If the client credentials are invalid, the function raises an exception.
        """
        if self.client_id == 'Insert_your_client_id_here' or self.client_id is None or self.client_secret == 'Insert_your_client_secret_here' or self.client_secret is None:
            print("*** Please set the CLIENT_ID & CLIENT_SECRET in environment variables or set Use_SSO to true. ***")
            raise Exception("Invalid client credentials")

class AuthenticationProviderWithClientSideTokenRefresh(httpx.Auth):
    def __init__(self):
        """
        Initializes the AuthenticationProviderWithTokenRefresh class.

        Initializes the client_id, client_secret, last_refreshed, and valid_until instance variables.
        """
        # Below properties are applicableto OAUTH only
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.last_refreshed = math.floor(time.time())
        self.valid_until = math.floor(time.time()) - 1
    
    def auth_flow(self, request):
        """
        Authenticates a request using either Single Sign-On or Client & Secret based on the value of USE_SSO.
        
        Parameters:
            request: The request object to authenticate.
        
        Returns:
            The authenticated request object.
        """
        if "x-correlation-id" not in request.headers:
            request.headers["x-correlation-id"] = str(uuid.uuid4())
        request.headers["Authorization"] = f"Bearer {self.get_bearer_token()}"
        yield request

    def get_bearer_token(self):
        """
        Returns the bearer token. If the current token has expired, it generates a new one using the client ID and secret.
        
        Returns:
            str: The generated or existing bearer token.
        """
        if self._is_expired():
            print("Generating new token...\n")
            self.last_refreshed = math.floor(time.time())
            _resp = auth.client_credentials(self.client_id, self.client_secret)
            self.token = _resp.token
            self.expires_in = _resp.expires_in
            self.valid_until = self.last_refreshed + self.expires_in
        else:
            print("Token not expired, using cached token...\n")
        return self.token

    def _is_expired(self):
        """
        Checks if the current time is greater than or equal to the valid_until attribute.

        Returns:
            bool: True if the current time is greater than or equal to valid_until, False otherwise.
        """
        return time.time() >= self.valid_until
    
def update_certifi():
    """Update Dell certificates if needed."""
    try:
        return
        url = "https://pki.dell.com//Dell%20Technologies%20PKI%202018%20B64_PEM.zip"
        print("Downloading Dell certificates zip from:", url)
        response = requests.get(url)
        response.raise_for_status()
        print("Downloaded certificate zip, size:", len(response.content), "bytes")

 
        cert_path = certifi.where()
        print("Certifi bundle path:", cert_path)

        dell_root_cert_name = "Dell Technologies Root Certificate Authority 2018.pem"
        dell_issuing_cert_name = "Dell Technologies Issuing CA 101_new.pem"

        
        print("Appending Dell certificates to certifi bundle...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
          
            root_cert_content = z.read(dell_root_cert_name).decode('utf-8')
            issuing_cert_content = z.read(dell_issuing_cert_name).decode('utf-8')

            with open(cert_path, "a") as bundle:
                bundle.write("\n")
                bundle.write(root_cert_content)
                bundle.write("\n")
                bundle.write(issuing_cert_content)
                bundle.write("\n")

        print("Dell certificates successfully added to certifi bundle.")

    except KeyError as e:
        print(f"Error: Certificate file '{e}' not found in the zip archive.")
    except Exception as e:
        print(f"An error occurred during certificate appending: {e}")