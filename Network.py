import oci
import pymysql 
from datetime import datetime, timedelta
import database
from Snow_response import servicenow_response
from details import logger
from pconst import const



table_name="cmdb_ci_network"

def get_network_details():
    logger.info(f"start fetching details from {table_name}")
    network_list=[]
    try:       
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        identity_client = oci.identity.IdentityClient({}, signer=signer)
        compartments = identity_client.list_compartments(signer.tenancy_id,lifecycle_state='ACTIVE')
        
        subscribed_regions = identity_client.list_region_subscriptions(signer.tenancy_id).data
        
        region_list=[reg.region_name for reg in subscribed_regions]        
        
        for compartment in compartments.data:
             
                try: 
                    for regions in region_list:
                        signer.region=regions
                        logger.info(f"start fetching {table_name} details from account -> {compartment.name} and region is ->{regions}")
                        network_client = oci.core.VirtualNetworkClient({}, signer=signer)
                        list_vcns_response = network_client.list_vcns(compartment_id=compartment.id)
                    
                        for network in list_vcns_response.data: 
                            network_response=network.__dict__

                            network_list.append({
                                'Display_name' :        network_response.get('_display_name',' '),
                                'State' :               network_response.get('_lifecycle_state',' '),
                                'Id'  :                 network_response.get('_id',' '),
                                'Cidr_block':           network_response.get('_cidr_block',' '),
                                'Domain_name'  :        network_response.get('_vcn_domain_name',' '),
                                'Account_id':           compartment.id or ' ',
                                'Datacenter':           signer.region or ' ',
                                'Tags' :                str(network_response.get('_defined_tags',' ').get('Oracle-Tags',' '))
                            })
                            
                except Exception as e:
                    print(f"Account name = {compartment.__dict__.get('_name',' ')} is not authorized:", e) 
                    logger.error(f"Error fetching in finding {table_name} details from {compartment.__dict__.get('_name',' ')} {str(e)}",exc_info=True)
                    servicenow_response(f"{const.EXECUTION_ERROR} fetching in finding {table_name} details from {compartment.__dict__.get('_name',' ')} : {str(e)}")
        logger.info(f"Successfully fetched details of {table_name}")
        insert_network_detail_into_db(network_list)
    except Exception as e:
        print("Error fetching instance data:", e)
        logger.error(f"Error fetching in {table_name} details : {e}",exc_info=True)
        servicenow_response(f"{const.EXECUTION_ERROR} fetching in {table_name} details : {e}")

def insert_network_detail_into_db(network_list):
    secret_data=database.get_secret_from_vault()
    print(secret_data)
    db_host=secret_data['db_host']
    db_user=secret_data['db_user']
    db_pass=secret_data['db_pass']
    db_name=secret_data['db_name']
    logger.info(f"Successfully fetched database credentials for {table_name}")
    try:
        connection=pymysql.connect(host=db_host,user=db_user,password=db_pass,database=db_name,cursorclass=pymysql.cursors.DictCursor)
        
        # table_name = 'cmdb_ci_network'

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
        CREATE TABLE IF NOT EXISTS cmdb_ci_network (
            Name varchar(100),
            State varchar(20),
            Object_id varchar(100),
            Cidr varchar(20),
            Domain_name varchar(50),
            Account_ID varchar(100),
            Datacenter varchar(20),
            Tags varchar(100)

        );"""


        cursor.execute(create_table)
    
        
        for item in network_list:
            insert_query = """
                INSERT INTO cmdb_ci_network(Name,State,Object_id,Cidr,Domain_name,Account_id,Datacenter,Tags) 
                values(%s,%s,%s,%s,%s,%s,%s,%s);
            """
            try:
                cursor.execute(insert_query,(item['Display_name'],item['State'],item['Id'],item['Cidr_block'],item['Domain_name'],item['Account_id'],item['Datacenter'],item['Tags']))
                
            except pymysql.Error as e:
                print(f"Error: {e}")
                logger.error(f"Error in {table_name} table : {e}",exc_info=True)
                servicenow_response(f"Error in {table_name} table : {e}")

            
        print("Data INSERT INTO cmdb_ci_network is successful")
        logger.info(f"Data INSERT INTO db for {table_name} is successful")
        connection.commit()
        connection.close()
    except Exception as e:
        print(f"Error inserting data into RDS: {str(e)}")      
        logger.error(f"Error inserting in {table_name} data into RDS: {str(e)}",exc_info=True) 
        servicenow_response(f"{const.EXECUTION_ERROR} inserting in {table_name} data into RDS: {str(e)}") 

if __name__=="__main__": 
    const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'
    get_network_details()