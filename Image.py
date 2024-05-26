import oci
import pymysql as mysql
from datetime import datetime, timedelta
import json
import database
import details
table_name="cmdb_ci_os_template"
# logger.error(f"Error fetching active compartments for {table_name}: {e}",exc_info=True)
def image_details():
        
    def fetch_active_compartments(identity_client, compartment_id):
        active_compartments = []
        try:
            compartments = identity_client.list_compartments(compartment_id, compartment_id_in_subtree=True, lifecycle_state='ACTIVE').data
            for compartment in compartments:
                if compartment.name == "ManagedCompartmentForPaaS":
                    continue
                active_compartments.append({
                    'compartment_name': compartment.name,
                    'compartment_id': compartment.id
                })
        except oci.exceptions.ServiceError as e:
            print("Error fetching active compartments:", e)
        return active_compartments

    def fetch_oci_image_details(config, signer, compartment_ids):
        image_details = []
        
        try:
            identity = oci.identity.IdentityClient(config={}, signer=signer)  # Create a new IdentityClient instance
            tenancy_id = signer.tenancy_id  # Get tenancy ID from the signer
            regions = identity.list_region_subscriptions(tenancy_id).data

            for compartment_id in compartment_ids:
                for region in regions:
                    config["region"] = region.region_name
                    compute = oci.core.ComputeClient(config=config, signer=signer)
                    instances = compute.list_instances(compartment_id).data
                    instance_image_map = {instance.id: instance.image_id for instance in instances}

                    for instance in instances:
                        image_id = instance_image_map.get(instance.id)
                        if image_id:
                            image = compute.get_image(image_id).data
                            image_info = {
                                'Name': image.display_name,
                                'Object Id': image.id,
                                'Version': image.operating_system_version,
                                'Guest OS': image.operating_system,
                                'Account ID': instance.compartment_id,
                                'Data Center': map_region_name(region.region_name),
                                'VM ObjectID': instance.id
                            }
                            tags = image.defined_tags.get('Oracle-Tags', {})
                            image_info['Tags'] = {} if not tags else tags
                            image_details.append(image_info)
        except oci.exceptions.ServiceError as e:
            print("Error fetching OCI image details:", e)

        return image_details

    def map_region_name(region_name):
        if region_name == "phx":
            return "us-phoenix-1"
        else:
            return region_name

    def db_connect_create_insert_details(config, signer):
        secret_data=database.get_secret_from_vault()
        print(secret_data)
        db_host=secret_data['db_host']
        db_user=secret_data['db_user']
        db_password=secret_data['db_pass']
        db_name=secret_data['db_name']

        try:
            connection = mysql.connect(host=db_host, user=db_user, password=db_password, database=db_name)
            print("Connected to MySQL database successfully!")

            with connection.cursor() as cursor:
                # table_name = "cmdb_ci_os_template"
                current_date = datetime.now()
                current_time = datetime.now().strftime("%H:%M:%S")
                previous_date = (current_date - timedelta(days=1)).strftime("%d-%m-%Y")

                show_table = f"SHOW TABLES LIKE '{table_name}'"
                cursor.execute(show_table)
                tb = cursor.fetchone()
                if tb:
                    rename_table_query = f"ALTER TABLE `{table_name}` RENAME TO `{table_name}_{previous_date}_{current_time}`"
                    cursor.execute(rename_table_query)
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    Name VARCHAR(500),
                    `Object Id` VARCHAR(500),
                    Version VARCHAR(500),
                    `Guest OS` VARCHAR(500),
                    AccountID VARCHAR(500),
                    DataCenter VARCHAR(500),
                    Tags JSON,
                    `VM ObjectID` VARCHAR(500)
                )
                """
                cursor.execute(create_table_query)
                print("Database table created successfully!")

                # Get identity client
                identity_client = oci.identity.IdentityClient(config={}, signer=signer)

                # Fetch compartment IDs
                compartment_ids = [compartment['compartment_id'] for compartment in fetch_active_compartments(identity_client, config["tenancy"])]

                # Fetch OCI image details
                oci_images = fetch_oci_image_details(config, signer, compartment_ids)

                # Insert OCI image details into database
                for detail in oci_images:
                    tags = json.dumps(detail['Tags']) if detail['Tags'] else None
                    insert_query = f"""
                    INSERT INTO {table_name} (Name, `Object Id`, Version, `Guest OS`, AccountID, DataCenter, Tags, `VM ObjectID`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (detail['Name'], detail['Object Id'], detail['Version'], detail['Guest OS'], detail['Account ID'], detail['Data Center'], tags, detail['VM ObjectID']))

                connection.commit()
                print("Details inserted into database successfully!")

        except mysql.Error as e:
            print(f"Error connecting to MySQL database or performing database operations: {e}")
        except oci.exceptions.ServiceError as e:
            print("Error fetching OCI image details:", e)
        finally:
            if 'connection' in locals() and connection.open:
                connection.close()

    # if __name__ == "__main__":
    config = {}
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    config["tenancy"] = signer.tenancy_id 

    # Execute fetch_oci_image_details
    identity_client = oci.identity.IdentityClient(config={}, signer=signer)
    compartment_ids = [compartment['compartment_id'] for compartment in fetch_active_compartments(identity_client, config["tenancy"])]
    fetch_oci_image_details(config, signer, compartment_ids)
    
    # Execute DB_connect_create_insert_details function finally
    db_connect_create_insert_details(config, signer)

if __name__ == "__main__":      
    image_details()
    



