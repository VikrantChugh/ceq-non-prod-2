import oci
import logging
from datetime import datetime, timedelta, timezone

current_date = datetime.now().strftime("%d-%m-%Y")
current_time = datetime.now().strftime("%H.%M.%S")
log_file_name = f"Logs/Oracle-discovery-logs-{current_date}-{current_time}.log"
logging.basicConfig(filename=log_file_name,level=logging.INFO,format='%(asctime)s  :  %(levelname)s  : %(message)s')

logger = logging.getLogger()

def account_region_details():
    region_list=[]
    account_list=[]
    region_count=0
    account_count=0
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner() 
    # Create a service client
    identity_client = oci.identity.IdentityClient({}, signer=signer)
    # config["tenancy"]="ocid1.tenancy.oc1..aaaaaaaakv76c6ktt3m4rv3pmovja4oh2lgyr7qvabsu2am3a2z35lejaiya"
    subscribed_regions = identity_client.list_region_subscriptions(signer.tenancy_id).data
    # print(subscribed_regions)
    compartments = identity_client.list_compartments(signer.tenancy_id)
    for compartment in compartments.data:
        if compartment.lifecycle_state=='ACTIVE':
            account_list.append(compartment.name)
            account_count=account_count+1

    logger.info("fetch data from these accounts {%s} ", account_list)
    logger.info(f"total accounts=%s",account_count)
    for subscribed_region in subscribed_regions:
        region_count=region_count+1
        region_list.append(subscribed_region.region_name)

    logger.info("fetch data from these region {%s} ", region_list)
    logger.info(f"total subscribed region=%s",region_count)

    # print(reg_list)
    # print(region_list)
    # print(region_count)
if __name__ =='__main__':
    account_region_details()
# Get the tenancy details
# tenancy = identity_client.get_tenancy(config["tenancy"])
# for m in tenancy.data:
#     print(m)
# Extract and print the tenancy ID
# tenancy_id = tenancy.data.id
# print("Tenancy ID:", tenancy_id)



       