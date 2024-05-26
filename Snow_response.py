import details 
import requests
from requests.auth import HTTPBasicAuth
import database
# import details
# from details import *
secret_data=database.get_secret_from_vault()
print(secret_data)
snow_password=secret_data['snow_api_password']
snow_username=secret_data['snow_api_username']

def servicenow_response(response):
	url = 'https://cloudeqincdemo2.service-now.com/api/ceq/oci_etl_discovery/Response_from_OCI'
      
	username = snow_username
	password = snow_password
	payload = {"oci_response" : response}

	result = requests.post(url, auth=HTTPBasicAuth(username, password), json=payload)
	if result.status_code == 200:
		# print(f'{response} : Successfully sent this response to ServiceNow.')
		details.logger.info('Successfully sent this response to ServiceNow : %s', response)
	else:
		print(f'{response} : Error in sending this response to ServiceNow.')
		details.logger.info(' Error in sending this response to ServiceNow : %s', response)

# message="*** Succesfully inserted data into table"
# servicenow_response(message)