import oci
import pymysql 
from datetime import datetime, timedelta
import database
from Snow_response import servicenow_response
from details import logger
from pconst import const

# const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'

table_name="cmdb_ci_cloud_database"

# Define a function to retrieve details of all VMs
def get_cloud_database_details():
    logger.info(f"start fetching details from {table_name}")
    try:
        # Initialize lists to store subnet and VM details    
        db_list=[] 

        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()

        # Initialize the IdentityClient to fetch service account details
        identity_client = oci.identity.IdentityClient({}, signer=signer)
        subscribed_regions = identity_client.list_region_subscriptions(signer.tenancy_id).data
        compartments = identity_client.list_compartments(signer.tenancy_id,lifecycle_state='ACTIVE')
        region_list=[reg.region_name for reg in subscribed_regions]
        
        
        # Loop through each compartment to get VM details
        for compartment in compartments.data:
            
                try:
                    for regions in region_list:
                        # print(compartment)
                        signer.region=regions
                        logger.info(f"start fetching {table_name} details from account -> {compartment.name} and region is ->{regions}")
                         # List the autonomous databases in the specified compartment
                
                        database_client = oci.database.DatabaseClient({}, signer=signer)
                        response = database_client.list_autonomous_databases(compartment.id)
                        
                        autonomous_databases = response.data
                        for autonomous_database in autonomous_databases:
                            # print(autonomous_database)
                            db_response=autonomous_database.__dict__
                            # name=db_response.get('_db_name',' ') + "@" + db_response.get('_display_name',' ')
                            # print(name)
                            # print( "oracle Autonomous" + "-" +  db_response.get('_db_workload',' '))
                            # print(db_response.get('_id',' '))
                            # print(db_response.get('_db_version',' '))
                            # print(db_response.get('_lifecycle_state',' '))
                            
                            # print(db_response.get('_id',' '))
                            db_list.append({
                                'Name' :         db_response.get('_db_name',' ') + "@" + db_response.get('_display_name',' '),
                                'Object_id'  :             db_response.get('_id',' '),
                                'Operational_status':   " ",
                                'Install_status'   :           ' ',
                                'Vendor' :            'ORACLE',
                                "Type":                "Oracle Autonomous"+"-"+db_response.get('_db_workload',' '),
                                "Version" :        db_response.get('_db_version',' '),
                                'State':         db_response.get('_lifecycle_state',' '),
                                'Comments':             compartment.id,
                                'Account_id':              compartment.id,
                                'Datacenter': signer.region
                            


                            })
                       

                except Exception as e:
                    print(f"Account name = {compartment.name} is not authorized:", e)
                    logger.error(f"Error fetching in finding {table_name} details from {compartment.__dict__.get('_name',' ')} {str(e)}",exc_info=True)
                    servicenow_response(f"{const.EXECUTION_ERROR} fetching in finding {table_name} details from {compartment.__dict__.get('_name',' ')} : {str(e)}")

        logger.info(f"Successfully fetched details of {table_name}")
        # print(db_list)
            #    Call function to insert VM details into database
        insert_cloud_db_details_into_database(db_list)

    except Exception as e:
        print("Error fetching instance data:", e)
        logger.error(f"Error fetching in {table_name} details : {e}",exc_info=True)
        servicenow_response(f"{const.EXECUTION_ERROR} fetching in {table_name} details : {e}")



        
# Define a function to insert VM details into the database
def insert_cloud_db_details_into_database(final_list):
    secret_data=database.get_secret_from_vault()
    # print(secret_data)
    db_host=secret_data['db_host']
    db_user=secret_data['db_user']
    db_pass=secret_data['db_pass']
    db_name=secret_data['db_name']
    
    try:
        # Connect to the MySQL database
        connection=pymysql.connect(host=db_host,user=db_user,password=db_pass,database=db_name,cursorclass=pymysql.cursors.DictCursor)
        logger.info(f"Successfully connected with mysql db for {table_name} table")
        # table_name = 'cmdb_ci_vm_instance'

        cursor = connection.cursor()

        # Get current date and time for renaming the table
        current_date = datetime.now()
        current_time = datetime.now().strftime("%H:%M:%S")
        previous_date = (current_date - timedelta(days=1)).strftime("%d-%m-%Y")

        # Check if table exists, if yes rename it
        show_table = f"SHOW TABLES LIKE '{table_name}'"
        cursor.execute(show_table)
        tb = cursor.fetchone()
        if tb:
            rename_table_query = f"ALTER TABLE `{table_name}` RENAME TO `{table_name}_{previous_date}_{current_time}`"
            cursor.execute(rename_table_query)

        # Create table if not exists
        create_table = """
        CREATE TABLE IF NOT EXISTS cmdb_ci_cloud_database (
            Name varchar(100),
            Object_id varchar(200),
            Operational_status varchar(100),
            Install_status varchar(100),
            Vendor varchar(100),
            Type varchar(100),
            Version varchar(100),
            State varchar(100),
            Comments varchar(200),
            Account_id varchar(200),
            Datacenter varchar(100)
            
        );"""

        cursor.execute(create_table)
    
        # Insert VM details into the database
        for item in final_list:
            insert_query = """
                INSERT INTO cmdb_ci_cloud_database(Name,Object_id,Operational_status,Install_status,Vendor,Type,Version,State,Comments,Account_id,Datacenter) 
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
            try:
                cursor.execute(insert_query,(item['Name'],item['Object_id'],item['Operational_status'],item['Install_status'],item['Vendor'],item['Type'],item['Version'],item['State'],item['Comments'],item['Account_id'],item['Datacenter']))
                
            except pymysql.Error as e:
                print(f"Error: {e}")
                logger.error(f"Error in {table_name} table : {e}")
                servicenow_response(f"Error in {table_name} table : {e}")

        print("Data INSERT INTO cmdb_ci_vm_instance is successful")
        logger.info(f"Data INSERT INTO db for {table_name} is successful")
        
        connection.commit()
        connection.close()
        logger.info(f"Successfully closed db connection for {table_name}")
    except Exception as e:
        print(f"Error inserting data into RDS: {str(e)}")   
        logger.error(f"Error inserting in {table_name} data into RDS: {str(e)}",exc_info=True) 
        servicenow_response(f"{const.EXECUTION_ERROR} inserting in {table_name} data into RDS: {str(e)}")

# Call the function to get VM details and insert into database
if __name__=="__main__":
    const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'
    get_cloud_database_details()
