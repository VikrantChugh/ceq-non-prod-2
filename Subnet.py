import oci
import pymysql 
from datetime import datetime, timedelta
from details import logger
import database
from Snow_response import servicenow_response
from pconst import const

# const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'

table_name="cmdb_ci_cloud_subnet"

def get_subnet_details():
    subnet_list=[]
    try:
        logger.info(f"start fetching details from {table_name}")
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        
       
        identity_client = oci.identity.IdentityClient({}, signer=signer)
        subscribed_regions = identity_client.list_region_subscriptions(signer.tenancy_id).data
        
        region_list=[reg.region_name for reg in subscribed_regions] 
        compartments = identity_client.list_compartments(signer.tenancy_id,lifecycle_state='ACTIVE')    
        
       
        for compartment in compartments.data:
             
                try:
                    for regions in region_list:
                        signer.region=regions
                        logger.info(f"start fetching {table_name} details from account -> {compartment.name} and region is ->{regions}")
                        subnet_client = oci.core.VirtualNetworkClient({}, signer=signer)
                        list_subnets_response = subnet_client.list_subnets(compartment_id=compartment.id)

                
                        for subnet in list_subnets_response.data:
                            subnet_response=subnet.__dict__                         
                            
                            
                            subnet_list.append({
                                'Display_name' : subnet_response.get('_display_name',' '),
                                'Id'  : subnet_response.get('_id',' '),
                                'Cidr_block':subnet_response.get('_cidr_block',' '),
                                'Domain_name'  : subnet_response.get('_subnet_domain_name',' '),
                                'State'   : subnet_response.get('_lifecycle_state',' '),
                                'Account_id':compartment.id,
                                'Datacenter': signer.region,
                                'Network_object_id':subnet_response.get('_vcn_id',' '),
                                'Tags': str(subnet_response.get('_defined_tags',' ').get('Oracle-Tags',' '))

                                })
                except Exception as e:
                    print(f"Account name = {compartment.__dict__.get('_name',' ')} is not authorized:", e)
                    # details.logger.error(f"Account name = {compartment.__dict__.get('_name',' ')} is not authorized: {e}",exc_info=True)
                    logger.error(f"Error fetching in finding {table_name} details from {compartment.__dict__.get('_name',' ')} {str(e)}",exc_info=True)
                    servicenow_response(f"{const.EXECUTION_ERROR} fetching in finding {table_name} details from {compartment.__dict__.get('_name',' ')} : {str(e)}")

        logger.info(f"Successfully fetched details of {table_name}")

        insert_subnet(subnet_list)
      
    except Exception as e:
        print("Error fetching {table_name} data:", e)
        logger.error(f"Error fetching in {table_name} details : {e}",exc_info=True)
        # print(f"{str(e)}")
        servicenow_response(f"{const.EXECUTION_ERROR} fetching in {table_name} details : {e}")



def insert_subnet(subnet_list):
    try:
        secret_data=database.get_secret_from_vault()
        db_host=secret_data['db_host']
        db_user=secret_data['db_user']
        db_pass=secret_data['db_pass']
        db_name=secret_data['db_name']

        logger.info(f"Successfully fetched database credentials for {table_name}")
        
        try:
            logger.info(f"connecting with mysql db for {table_name} table")

            connection=pymysql.connect(host=db_host,user=db_user,password=db_pass,database=db_name,cursorclass=pymysql.cursors.DictCursor)

            try:
                logger.info(f"Successfully connected with mysql db for {table_name} table")
                # table_name = 'cmdb_ci_cloud_subnet'

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
                CREATE TABLE IF NOT EXISTS cmdb_ci_cloud_subnet (
                    Name varchar(100),
                    Object_id varchar(100),
                    Cidr varchar(50),
                    Domain_name varchar(100),
                    State varchar(50),
                    Account_id varchar(100),
                    Datacenter varchar(50),
                    Network_object_id varchar(100),
                    Tags varchar(200)

                );"""


                cursor.execute(create_table)
            
                
                for item in subnet_list:
                    insert_query = """
                        INSERT INTO cmdb_ci_cloud_subnet(Name,Object_id,Cidr,Domain_name,State,Account_id,Datacenter,Network_object_id,Tags) 
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """
                    try:
                        cursor.execute(insert_query,(item['Display_name'],item['Id'],item['Cidr_block'],item['Domain_name'],item['State'],item['Account_id'],item['Datacenter'],item['Network_object_id'],item['Tags']))
                        
                    except pymysql.Error as e:
                        print(f"Error: {e}")
                        logger.error(f"Error in {table_name} table : {e}",exc_info=True)
                        servicenow_response(f"Error in {table_name} table : {e}")


                print(f"Data INSERT INTO {table_name} is successful")
                logger.info(f"Data INSERT INTO db for {table_name} is successful")
                
                # servicenow_response(f"Data INSERT INTO db for {table_name} is successful")

                connection.commit()
                connection.close()
                logger.info(f"Successfully closed db connection for {table_name}")

            except Exception as e:
                print(f"Error inserting in {table_name} data into RDS: {str(e)}")  
                logger.error(f"Error inserting in {table_name} data into RDS: {str(e)}",exc_info=True) 
                servicenow_response(f"{const.EXECUTION_ERROR} inserting in {table_name} data into RDS: {str(e)}")

        except Exception as e:
            print(f"Wrong database credentials: {e}")
            logger.error(f"Wrong database credentials: {e}",exc_info=True)
            servicenow_response(f"{const.EXECUTION_ERROR} Wrong database credentials : {str(e)}")

        # except Exception as e:
        #     print(f"Error inserting in {table_name} data into RDS: {str(e)}")  
        #     logger.error(f"Error inserting in {table_name} data into RDS: {str(e)}") 
        #     servicenow_response(f"{const.EXECUTION_ERROR} inserting in {table_name} data into RDS: {str(e)}")

    except Exception as e:
        logger.error(f"Error fetching in database credentials: {str(e)}",exc_info=True)
        servicenow_response(f"{const.EXECUTION_ERROR} fetching in database credentials: {str(e)}")

if __name__=="__main__":
    const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'
    get_subnet_details()
