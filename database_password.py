import oci
from oci.auth import signers
import base64

def get_secret_from_vault():
    # Initialize the signer using Instance Principals
    signer = signers.InstancePrincipalsSecurityTokenSigner()

    # Initialize the VaultClient with the signer
    vault_client = oci.secrets.SecretsClient({}, signer=signer)

    # Set the compartment OCID where the secret is stored
    compartment_id = 'ocid1.compartment.oc1..aaaaaaaa7nxivmvn7wff2j4azbwncx4ywnmfuhx4eugo55huwwuozxysdw4a'

    # Set the secret OCID
    secret_id = 'ocid1.vaultsecret.oc1.ap-mumbai-1.amaaaaaabfgevmaa43hybdoxsdmbyvzma2stffounarrvh4ytquye26bxtcq'

    try:
        # Retrieve the secret from the Vault
        secret_bundle = vault_client.get_secret_bundle(secret_id).data

        # Extract the secret value
        db_pass = secret_bundle.secret_bundle_content.content
        password=base64_to_plain_text(db_pass)
        return password
    except Exception as e:
        print("Error retrieving secret from Vault:", e)
        return None

# print(type(get_secret_from_vault()))


def base64_to_plain_text(base64_string):
    try:
        # Decode the base64 string into bytes
        decoded_bytes = base64.b64decode(base64_string)
        # Convert bytes to string using UTF-8 encoding
        plain_text = decoded_bytes.decode('utf-8')
        return plain_text
    except Exception as e:
        print("Error:", e)
        return None

# Example usage
# get_secret_from_vault()
# password = base64_to_plain_text(base64_string)
# print(p)


