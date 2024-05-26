import oci
from oci.auth import signers
import base64
from details import logger


def get_secret_from_vault():
    # Initialize the signer using Instance Principals
    signer = signers.InstancePrincipalsSecurityTokenSigner()
    compartment_id = 'ocid1.compartment.oc1..aaaaaaaa7nxivmvn7wff2j4azbwncx4ywnmfuhx4eugo55huwwuozxysdw4a'
    # Initialize the VaultClient with the signer
    vault_client = oci.secrets.SecretsClient({}, signer=signer)
    
   
   
    # Set the compartment OCID where the secret is stored
    
    l=[]
    # Set the secret OCID
    # secret_id = 'ocid1.vaultsecret.oc1.ap-mumbai-1.amaaaaaabfgevmaa43hybdoxsdmbyvzma2stffounarrvh4ytquye26bxtcq'
    db_host1='ocid1.vaultsecret.oc1.ap-mumbai-1.amaaaaaabfgevmaa3nuaiztjxtcs3l4ewmjuwwdwrsi7rzohb2qb5ft4twoa'
    db_pass1='ocid1.vaultsecret.oc1.ap-mumbai-1.amaaaaaabfgevmaai62a22si5nybbh4p3qujj3ot4wbin6agcpcioqb77umq'
    db_name1='ocid1.vaultsecret.oc1.ap-mumbai-1.amaaaaaabfgevmaazhtlju6qbtcp4cmfhwzuvdpra3ourkwkmbw25mtnt4wa'
    db_user1='ocid1.vaultsecret.oc1.ap-mumbai-1.amaaaaaabfgevmaaprvnwoz6mwt45z5lu2hbcyazvuaywlytd7am4nkrrpta'
    # secret_id_list=[db_host1,db_pass1,db_name1,db_user1]
    snow_password='ocid1.vaultsecret.oc1.ap-mumbai-1.amaaaaaabfgevmaa7usdpny7czvgqenferzf6rrub555vaonzzu7jc2rytzq'
    snow_username='ocid1.vaultsecret.oc1.ap-mumbai-1.amaaaaaabfgevmaa7bbctugqtsmll7cty5o4z3pukle3duoe7odjznfra62a'
    secret_id_list=[db_host1,db_pass1,db_name1,db_user1,snow_password,snow_username]

    try:
        for m in secret_id_list:
            # print(m)
        # Retrieve the secret from the Vault
            secret_bundle = vault_client.get_secret_bundle(m).data

            # Extract the secret value
            db_pass = secret_bundle.secret_bundle_content.content
            password=base64_to_plain_text(db_pass)
            # print(password)
            l.append(password)

        # print(l)  
        details={
            'db_host':l[0],
            'db_pass':l[1],
            'db_name':l[2],
            'db_user':l[3],
            'snow_api_password':l[4],
            'snow_api_username':l[5]


        }
        
        # print(details)
        return details
            
    except Exception as e:
        print("Error retrieving secret from Vault:", e)
        logger.error(f"Error fetching in database credentials: {str(e)}")
            

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

if __name__=="__main__":
    secret_data=get_secret_from_vault()

