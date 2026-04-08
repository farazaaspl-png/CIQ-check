import io
import zipfile
import certifi
import requests
import os

def update_certifi():
    try:
        url = "https://pki.dell.com//Dell%20Technologies%20PKI%202018%20B64_PEM.zip"
        response = requests.get(url, verify=False)
        response.raise_for_status()

        cert_path = certifi.where()
        dell_root_cert_name = "Dell Technologies Root Certificate Authority 2018.pem"
        dell_issuing_cert_name = "Dell Technologies Issuing CA 101_new.pem"

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            root_cert_content = z.read(dell_root_cert_name).decode('utf-8')
            issuing_cert_content = z.read(dell_issuing_cert_name).decode('utf-8')

        # Make writable copy of certifi bundle (since original may be read-only)
        custom_bundle_path = "/etc/ssl/certs/ca-certificates.crt"
        os.makedirs(os.path.dirname(custom_bundle_path), exist_ok=True)

        with open(cert_path, "r") as src, open(custom_bundle_path, "w") as dst:
            dst.write(src.read())

        with open(custom_bundle_path, "a") as bundle:
            bundle.write("\n" + root_cert_content + "\n" + issuing_cert_content + "\n")

        print(f"✅ Certificates appended successfully to {custom_bundle_path}")

    except KeyError as e:
        print(f"❌ Error: Certificate file {e} not found in the zip archive.")
    except Exception as e:
        print(f"❌ An error occurred during certificate appending: {e}")

if __name__ == "__main__":
    update_certifi()


