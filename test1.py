import threading
# from ServiceAccount import get_service_account_details
from AvailabilityZones import get_availability_zone_details
from StorageVolume import get_storage_volume_details
# from VirtualMachine import get_virtual_machine_details
# from Network import get_network_details
# from Subnet import get_subnet_details
# from DataCenter import db_connect_create_insert_details
# from Image import image_details
# from Vnic import vnic_details
import details 

import requests
from requests.auth import HTTPBasicAuth
import database
import Snow_response
from pconst import const
const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'
# import details
# from details import *
# secret_data=database.get_secret_from_vault()
# print(secret_data)
# snow_password=secret_data['snow_api_password']
# def servicenow_response(response):
# 	url = 'https://cloudeqincdemo2.service-now.com/api/ceq/oci_etl_discovery/Response_from_OCI'
      
# 	username = 'CEQ_MID_OCI' 
# 	password = snow_password
# 	payload = {"oci_response" : response}

# 	result = requests.post(url, auth=HTTPBasicAuth(username, password), json=payload)
# 	if result.status_code == 200:
# 		print(f'{response} : Successfully sent this response to ServiceNow.')
# 		details.logger.info('%s : Successfully sent this response to ServiceNow.', response)
# 	else:
# 		print(f'{response} : Error in sending this response to ServiceNow.')
# 		details.logger.info('%s : Error in sending this response to ServiceNow.', response)
def main():
    try:     
        details.account_region_details()          
        # service_account_thread =threading.Thread(target=get_service_account_details)
        availability_zone_thread =threading.Thread(target=get_availability_zone_details)
        storage_volume_thread =threading.Thread(target=get_storage_volume_details)
        # virtual_machine_thread =threading.Thread(target=get_virtual_machine_details)
        # network_thread =threading.Thread(target=get_network_details)
        # subnet_thread =threading.Thread(target=get_subnet_details)
        # datacenter_thread=threading.Thread(target=db_connect_create_insert_details)
        # image_thread =threading.Thread(target=image_details)
        # vnic_thread =threading.Thread(target=vnic_details)

        # Zone =threading.Thread(target=get_availability_zone_details)
        # Zone =threading.Thread(target=get_availability_zone_details)
        details.logger.info("output from main")
        # subnet_thread.start()
        # subnet_thread.join()
        


        # service_account_thread.start()  
        # service_account_thread.join()
        availability_zone_thread.start()
        availability_zone_thread.join()
        storage_volume_thread.start()
        storage_volume_thread.join()
        # network_thread.start()
        # network_thread.join()
        # # service_account_thread.join()
        # # availability_zone_thread.join()
        # # storage_volume_thread.join()
        
        # virtual_machine_thread.start()
        # # network_thread.start()
        # # subnet_thread.start()
        
        # virtual_machine_thread.join()
        # # network_thread.join()
        # # subnet_thread.join()

        # datacenter_thread.start()
        # datacenter_thread.join()
        # image_thread.start()
        # image_thread.join()
        # vnic_thread.start()
        # vnic_thread.join()
        # message="*** Succesfully inserted data into table"
        # # print(message)
        # Snow_response.servicenow_response(message)
    except Exception as e:
        print("Error fetching in main file:", e)

main()