import oci
import pymysql as mysql
from datetime import datetime, timedelta
import database
from Snow_response import servicenow_response
from details import logger
from pconst import const

# const.EXECUTION_ERROR = 'OCI Cloud Ingestion Engine Error'

table_name="cmdb_ci_oci_datacenter"


# table_name="Datacenter"

def fetch_active_compartments(identity_client, compartment_id):
    active_compartments = []
    try:
        compartments = identity_client.list_compartments(compartment_id, compartment_id_in_subtree=True, lifecycle_state='ACTIVE').data
        for compartment in compartments:
            active_compartments.append({
                'compartment_name': compartment.name,
                'compartment_id': compartment.id
            })
    except Exception as e:
        print("Error fetching active compartments:", e)
        logger.error(f"Error fetching active compartments for {table_name}: {e}",exc_info=True)
    return active_compartments

def fetch_oci_datacenters(signer):
    logger.info(f"start fetching details from {table_name}")
    datacenters = []
    try:
        identity_client = oci.identity.IdentityClient({}, signer=signer)
        tenancy_id = signer.tenancy_id
        subscribed_regions = identity_client.list_region_subscriptions(tenancy_id).data

        for subscribed_region in subscribed_regions:
            region_name = subscribed_region.region_name
            compartment_id = tenancy_id

            datacenters.append({
                'name' : region_name,
                'region': region_name,
                'object_id' : region_name,
                'Account ID': tenancy_id
            })

            active_compartments = fetch_active_compartments(identity_client, compartment_id)
            for compartment in active_compartments:
                logger.info(f"start fetching {table_name} details from account -> {compartment['compartment_name']} and region is ->{region_name}")
                datacenters.append({
                    'name' : region_name,
                    'region': region_name,
                    'object_id' : region_name,
                    'Account ID': compartment['compartment_id']
                })

    except Exception as e:
        print("Error fetching OCI data centers:", e)
        logger.error(f"Error fetching OCI data centers of {table_name}:{e}",exc_info=True)
    return datacenters

def db_connect_create_insert_details():
    logger.info(f"start fetching details from {table_name}")
    secret_data=database.get_secret_from_vault()
    print(secret_data)
    db_host=secret_data['db_host']
    db_user=secret_data['db_user']
    db_password=secret_data['db_pass']
    db_name=secret_data['db_name']

    try:
        # Connect to MySQL database
        connection = mysql.connect(host=db_host, user=db_user, password=db_password, database=db_name)
        print("Connected to MySQL database successfully!")
        logger.info(f"Successfully connected with mysql db for {table_name} table")
        with connection.cursor() as cursor:
            # Create MySQL database table if not exists
            # table_name = "cmdb_ci_oci_datacenter"
            current_date = datetime.now()
            current_time = datetime.now().strftime("%H:%M:%S")
            previous_date = (current_date - timedelta(days=1)).strftime("%d-%m-%Y")

            show_table = f"SHOW TABLES LIKE '{table_name}'"
            cursor.execute(show_table)
            tb = cursor.fetchone()
            if tb:
                rename_table_query = f"ALTER TABLE `{table_name}` RENAME TO `{table_name}_{previous_date}_{current_time}`"
                cursor.execute(rename_table_query)
            create_table_query = """
            CREATE TABLE IF NOT EXISTS cmdb_ci_oci_datacenter (
                name VARCHAR(500),
                region VARCHAR(500), 
                object_id VARCHAR(500),
                `Account ID` VARCHAR(500)
            )
            """
            cursor.execute(create_table_query)
            print("Database table created successfully!")

            # Fetch OCI data centers
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            oci_datacenters = fetch_oci_datacenters(signer)
            logger.info(f"Successfully fetched details of {table_name}")

            # Insert OCI data center details into MySQL database
            for detail in oci_datacenters:
                insert_query = """
                INSERT INTO cmdb_ci_oci_datacenter (name, region, object_id, `Account ID`)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (detail['name'], detail['region'], detail['object_id'], detail['Account ID']))

            connection.commit()
            print("Details inserted into database successfully!")
            logger.info(f"Data INSERT INTO db for {table_name} is successful")

    except Exception as e:
        print(f"Error connecting to MySQL database or performing database operations: {e}")
        logger.error(f"Error connecting to MySQL database or performing database operations for {table_name}: {e}",exc_info=True)
        servicenow_response(f"Error connecting to MySQL database or performing database operations for {table_name}: {e}")

    # except Exception as e:
    #     print("Error fetching OCI data centers:", e)
    #     logger.error(f"rror fetching OCI data centers of {table_name}:{e}",exc_info=True)
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

if __name__ == "__main__":
    # Call the combined function to connect to MySQL, create table, and insert details
    db_connect_create_insert_details()




