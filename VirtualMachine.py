import oci
import pymysql 
from datetime import datetime, timedelta
import database
from Snow_response import servicenow_response
from details import logger
from pconst import const

# const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'

table_name="cmdb_ci_vm_instance"

# Define a function to retrieve details of all VMs
def get_virtual_machine_details():
    logger.info(f"start fetching details from {table_name}")
    try:
        # Initialize lists to store subnet and VM details    
        vm_list=[] 

        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()

        identity_client = oci.identity.IdentityClient({}, signer=signer)

        compartments = identity_client.list_compartments(signer.tenancy_id,lifecycle_state='ACTIVE')
        # Use Instance Principals Security Token Signer for authentication
        subscribed_regions = identity_client.list_region_subscriptions(signer.tenancy_id).data        
        region_list=[reg.region_name for reg in subscribed_regions]
        
        
        # Loop through each compartment to get VM details
        for compartment in compartments.data:
            
                try:
                    for regions in region_list:
                        signer.region=regions
                        logger.info(f"start fetching {table_name} details from account -> {compartment.name} and region is ->{regions}")
                        compute_client = oci.core.ComputeClient({}, signer=signer)
                       
                        identity_client = oci.identity.IdentityClient({}, signer=signer)      
                       
                        compartment_id=signer.tenancy_id
                        
                        
                        availability_domains = identity_client.list_availability_domains(compartment_id).data
                        list_instances_response = compute_client.list_instances(compartment.id)

                       

                        for instance in list_instances_response.data:                        
                            
                            instance_response = instance.__dict__
                            
                            list_subnet = compute_client.list_vnic_attachments(compartment.id,instance_id=instance.id) 
                            
                            check=0
                            for subnets in list_subnet.data: 
                                if check==1:
                                    break
                                 

                                if len(list_subnet.data) == 2:
                                    if list_subnet.data[1].subnet_id==subnets.subnet_id:
                                        check=1   
                           
                                for availability_domain in availability_domains:
                                    if instance.availability_domain==availability_domain.name:
                                        availability_zone_object_id= availability_domain.id
                                        break
                                fault_domains = identity_client.list_fault_domains(compartment_id, instance.availability_domain).data
                                for fd in fault_domains:
                                    if instance.fault_domain==fd.name:
                                        fault_domain=fd.id
                                        break
                                vm_list.append({
                                    'Object_id' :         instance_response.get('_id',' '),
                                    'Name'  :             instance_response.get('_display_name',' '),
                                    'Avalibility_zone':   instance_response.get('_availability_domain',' '),
                                    'State'   :           instance_response.get('_lifecycle_state',' '),
                                    'Memory' :            instance_response.get('_shape_config',' ').__dict__.get('_memory_in_gbs',' '),
                                    "Cpu":                instance_response.get('_shape_config',' ').__dict__.get('_ocpus',' '),
                                    "Account_id" :        compartment.id or ' ',
                                    'Datacenter':         signer.region or ' ',
                                    'Subnet':             subnets.subnet_id,
                                    'Tags':               str(instance_response.get('_defined_tags',' ').get('Oracle-Tags',' ')),
                                    'Vnic_attachment_id': subnets.id,
                                    "Availability_zone_object_id": availability_zone_object_id,
                                    "Fault_domain":       fault_domain,
                                    "Fault_domain_name":  instance.fault_domain


                                })
                        

                except Exception as e:
                    print(f"Account name = {compartment.name} is not authorized:", e)
                    logger.error(f"Error fetching in finding {table_name} details from {compartment.__dict__.get('_name',' ')} {str(e)}",exc_info=True)
                    servicenow_response(f"{const.EXECUTION_ERROR} fetching in finding {table_name} details from {compartment.__dict__.get('_name',' ')} : {str(e)}")

        logger.info(f"Successfully fetched details of {table_name}")
                # Call function to insert VM details into database
        insert_vm_details_into_database(vm_list)

    except Exception as e:
        print("Error fetching instance data:", e)
        logger.error(f"Error fetching in {table_name} details : {e}",exc_info=True)
        servicenow_response(f"{const.EXECUTION_ERROR} fetching in {table_name} details : {e}")



        
# Define a function to insert VM details into the database
def insert_vm_details_into_database(final_list):
    secret_data=database.get_secret_from_vault()
    print(secret_data)
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
        CREATE TABLE IF NOT EXISTS cmdb_ci_vm_instance (
            Name varchar(100),
            Object_id varchar(100),
            State varchar(100),
            Memory varchar(100),
            Cpu varchar(100),
            Account_id varchar(100),
            Datacenter varchar(100),
            Avalibility_zone varchar(100),
            Subnet varchar(100),
            Tags varchar(200),
            Vnic_attachment_id varchar(100),
            Availability_zone_object_id varchar(100),
            Fault_domain varchar(100),
            Fault_domain_name varchar(100)
        );"""

        cursor.execute(create_table)
    
        # Insert VM details into the database
        for item in final_list:
            insert_query = """
                INSERT INTO cmdb_ci_vm_instance(Name,Object_id,State,Memory,Cpu,Account_id,Datacenter,Avalibility_zone,Subnet,Tags,Vnic_attachment_id,Availability_zone_object_id,Fault_domain,Fault_domain_name) 
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
            try:
                cursor.execute(insert_query,(item['Name'],item['Object_id'],item['State'],item['Memory'],item['Cpu'],item['Account_id'],item['Datacenter'],item['Avalibility_zone'],item['Subnet'],item['Tags'],item['Vnic_attachment_id'],item['Availability_zone_object_id'],item['Fault_domain'],item['Fault_domain_name']))
                
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
    get_virtual_machine_details()