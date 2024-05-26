import oci
import pymysql 
from datetime import datetime, timedelta
import database
from Snow_response import servicenow_response
from details import logger
from pconst import const

# const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'

table_name="cmdb_ci_cloud_service_account"

def get_service_account_details():
    logger.info(f"start fetching details from {table_name}")
    account_list = []
    try:
        # Load the configuration and initialize signer
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        
        # Initialize the IdentityClient to fetch service account details
        identity_client = oci.identity.IdentityClient({}, signer=signer)

        # Get details of the tenancy
        tenancy = identity_client.get_tenancy(signer.tenancy_id).data
        # Convert object into dictionary type
        tenancy_response = tenancy.__dict__
        tenancy_id = tenancy.id
        # Check if the tenancy is the master account
        master_account = "Yes" if tenancy_id == signer.tenancy_id else "No"
        
        # Append tenancy details to account_list
        account_list.append({
            'Name': tenancy_response.get('_name',' '),
            'Account_id': tenancy_response.get('_id',' '),
            'Object_id': tenancy_response.get('_id',' '),
            'Organization_id': tenancy_response.get('_id',' '),
            'Is_master_account': master_account ,
            'Tags': str(tenancy_response.get('_defined_tags',' ').get('Oracle-Tags',' '))
        })
        
        # Get the list of compartments
        compartments = identity_client.list_compartments(signer.tenancy_id,lifecycle_state='ACTIVE')
        
        # Iterate through compartments
        for compartment in compartments.data:  
            # Convert object into dictionary type
            logger.info(f"start fetching {table_name} details from account -> {compartment.name}")
            compartment_response = compartment.__dict__
            compartment_id=compartment.id

            # Check if the compartment is the master account
            master_account = "Yes" if compartment_id == signer.tenancy_id else "No"

            # Check if compartment is active
           
            # Append compartment details to account_list
            account_list.append(
                {
                    'Name': compartment_response.get('_name', ' '),
                    'Account_id': compartment_response.get('_id', ' '),
                    'Object_id':  compartment_response.get('_id', ' '),
                    'Organization_id': tenancy_response.get('_id',' '),
                    'Is_master_account': master_account ,
                    'Tags': str(compartment_response.get('_defined_tags',' ').get('Oracle-Tags',' '))
                }
            )

        logger.info(f"Successfully fetched details of {table_name}")
        # Insert service account details into the database
        insert_service_account_details_into_database(account_list)
    except oci.exceptions.ServiceError as e:
        print("Error fetching active compartments", e)
        logger.error(f"Error fetching in {table_name} details : {e}",exc_info=True)
        servicenow_response(f"{const.EXECUTION_ERROR} fetching in {table_name} details : {e}")

# Function to insert service account details into the database
def insert_service_account_details_into_database(account_list):
    secret_data=database.get_secret_from_vault()
    print(secret_data)
    db_host=secret_data['db_host']
    db_user=secret_data['db_user']
    db_pass=secret_data['db_pass']
    db_name=secret_data['db_name']   
    try:
        # Establish database connection
        connection = pymysql.connect(host=db_host, user=db_user, password=db_pass, database=db_name, cursorclass=pymysql.cursors.DictCursor)
        # table_name = 'cmdb_ci_cloud_service_account'
        logger.info(f"Successfully connected with mysql db for {table_name} table")

        cursor = connection.cursor()

        # Get current time and previous date
        current_time = datetime.now().strftime("%H:%M:%S")
        previous_date = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")
        
        # Check if table exists and rename if it does
        show_table = f"SHOW TABLES LIKE '{table_name}'"
        cursor.execute(show_table)
        tb = cursor.fetchone()
        if tb:
            rename_table_query = f"ALTER TABLE `{table_name}` RENAME TO `{table_name}_{previous_date}_{current_time}`"
            cursor.execute(rename_table_query)

        # Create table if not exists
        create_table = """
        CREATE TABLE IF NOT EXISTS cmdb_ci_cloud_service_account (
            Name varchar(100),
            Account_id varchar(100),
            Object_id varchar(100),
            Is_master_account varchar(10),
            Organization_id varchar(100),
            Tags varchar(200)
         );"""

        cursor.execute(create_table)
        
        # Insert data into database
        for item in account_list:
            insert_query = """
                INSERT INTO cmdb_ci_cloud_service_account(Name,Account_id,Object_id,Is_master_account,Organization_id,Tags) 
                values(%s,%s,%s,%s,%s,%s);
            """
            try:
                cursor.execute(insert_query,(item['Name'], item['Account_id'], item['Object_id'], item['Is_master_account'], item['Organization_id'], item['Tags']))
                
            except pymysql.Error as e:
                print(f"Error: {e}")
                logger.error(f"Error in {table_name} table : {e}",exc_info=True)
                servicenow_response(f"Error in {table_name} table : {e}")


        print("Data INSERT INTO cmdb_ci_cloud_service_account is successful")
        logger.info(f"Data INSERT INTO db for {table_name} is successful")
        connection.commit()

        connection.close()
        logger.info(f"Successfully closed db connection for {table_name}")
    except Exception as e:
        print(f"Error inserting data into RDS: {str(e)}") 
        logger.error(f"Error inserting in {table_name} data into RDS: {str(e)}",exc_info=True) 
        servicenow_response(f"{const.EXECUTION_ERROR} inserting in {table_name} data into RDS: {str(e)}")

    
# Call function to fetch service account details
if __name__=="__main__":
    const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'
    get_service_account_details()