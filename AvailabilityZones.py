import oci
import pymysql 
from datetime import datetime, timedelta
from details import logger
import database
from Snow_response import servicenow_response
from pconst import const

# const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'

table_name="cmdb_ci_availability_zone"



def get_availability_zone_details():

    logger.info(f"start fetching details from {table_name}")
    zone_list=[]
    try:
        # Load the configuration
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        # Initialize the IdentityClient to fetch service account details
        identity_client = oci.identity.IdentityClient({}, signer=signer)
        subscribed_regions = identity_client.list_region_subscriptions(signer.tenancy_id).data
        compartments = identity_client.list_compartments(signer.tenancy_id,lifecycle_state='ACTIVE')
        region_list=[reg.region_name for reg in subscribed_regions]
        
        for compartment in compartments.data:
                            
                for regions in region_list:
                    signer.region=regions
                    logger.info(f"start fetching {table_name} details from account -> {compartment.name} and region is ->{regions}")
                    identity_client = oci.identity.IdentityClient({}, signer=signer)      
                    
                    compartment_id=compartment.id
                   
                    
                    availability_domains = identity_client.list_availability_domains(compartment_id).data

                    # List availability domains in the region    
                    for availability_domain in availability_domains:                        
                        availability_domain_id=availability_domain.id
                        availability_domain_name=availability_domain.name
                       
                            
                    # Extract the desired fields from the response data
                        zone_list.append({
                                'Object_id':availability_domain_id or ' ',
                                'Name':availability_domain_name or ' ',
                                'Account_id':compartment_id or ' ',
                                'Datacenter':signer.region or ' ',
                                'State': "AVAILABLE" or ' '
                                
                            })
                
        logger.info(f"Successfully fetched details of {table_name}")

        insert_availability_zone_details_into_database(zone_list)
        
        
          
    except Exception as e:
        print("Error fetching availability domains:", e)
        logger.error(f"Error fetching in {table_name} details : {e}",exc_info=True)
        servicenow_response(f"{const.EXECUTION_ERROR} fetching in {table_name} details : {e}")


def insert_availability_zone_details_into_database(zone_list):

    secret_data=database.get_secret_from_vault()
    # print(secret_data)
    db_host=secret_data['db_host']
    db_user=secret_data['db_user']
    db_pass=secret_data['db_pass']
    db_name=secret_data['db_name']

    logger.info(f"Successfully fetched database credentials for {table_name}")
    try:
        logger.info(f"connecting with mysql db for {table_name} table")
        connection=pymysql.connect(host=db_host,user=db_user,password=db_pass,database=db_name,cursorclass=pymysql.cursors.DictCursor)
        
        logger.info(f"Successfully connected with mysql db for {table_name} table")
        # table_name = 'cmdb_ci_availability_zone'
        cursor = connection.cursor()
        current_date = datetime.now()
        current_time = datetime.now().strftime("%H:%M:%S")
        previous_date = (current_date - timedelta(days=1)).strftime("%d-%m-%Y")

        show_table = f"SHOW TABLES LIKE '{table_name}'"
        cursor.execute(show_table)
        tb = cursor.fetchone()
        if tb:
            rename_table_query = f"ALTER TABLE `{table_name}` RENAME TO `{table_name}_{previous_date}_{current_time}`"
            cursor.execute(rename_table_query)
 
        create_table = """
        CREATE TABLE IF NOT EXISTS cmdb_ci_availability_zone (
            Name varchar(50),
            Object_id varchar(100),
            Account_id varchar(100),
            Datacenter varchar(50),
            State varchar(10)
           
           
        );"""
        cursor.execute(create_table)
         
        for item in zone_list:
            insert_query = """
                INSERT INTO cmdb_ci_availability_zone(Name,Object_id,Account_id,Datacenter,State) 
                values(%s,%s,%s,%s,%s);
            """
            try:
                cursor.execute(insert_query,(item['Name'],item['Object_id'],item['Account_id'],item['Datacenter'],item['State']))
                
            except pymysql.Error as e:
                print(f"Error: {e}")
                logger.error(f"Error in {table_name} table : {e}")
                servicenow_response(f"Error in {table_name} table : {e}")

        print("Data INSERT INTO cmdb_ci_availability_zone is successful")
        logger.info(f"Data INSERT INTO db for {table_name} is successful")
        connection.commit()
        connection.close()
        logger.info(f"Successfully closed db connection for {table_name}")

    except Exception as e:
        print(f"Error inserting data into RDS: {str(e)}")
        logger.error(f"Error inserting in {table_name} data into RDS: {str(e)}",exc_info=True) 
        servicenow_response(f"{const.EXECUTION_ERROR} inserting in {table_name} data into RDS: {str(e)}")

if __name__=="__main__":
    const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'
    get_availability_zone_details()