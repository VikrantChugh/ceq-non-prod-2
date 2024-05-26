import oci
import pymysql as mysql
import json
from datetime import datetime, timedelta
import database
import details

def vnic_details():
        
    # Initialize signer
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()

    # Initialize IdentityClient to fetch service account details
    identity_client = oci.identity.IdentityClient({}, signer=signer)

    # Get details of the tenancy
    tenancy = identity_client.get_tenancy(signer.tenancy_id).data
    tenancy_id = tenancy.id

    def fetch_active_compartments(identity_client, compartment_id):
        try:
            compartments = identity_client.list_compartments(compartment_id, compartment_id_in_subtree=True, lifecycle_state='ACTIVE').data
            print("Successfully fetched compartments.")
            return [compartment for compartment in compartments if compartment.name != "ManagedCompartmentForPaaS"]
        except oci.exceptions.ServiceError as e:
            print("Failed to fetch compartments:", e)
            return []

    def get_vnic_tags(network_client, vnic_id):
        try:
            vnic = network_client.get_vnic(vnic_id).data
            return vnic.defined_tags.get("Oracle-Tags", {})
        except oci.exceptions.ServiceError as e:
            print("Failed to fetch VNIC tags:", e)
            return {}

    def fetch_vnic_details(network_client, compute_client, vnic_attachment, region):
        try:
            vnic = network_client.get_vnic(vnic_attachment.vnic_id).data
            tags = get_vnic_tags(network_client, vnic.id)
            return {
                "Name": vnic.display_name,
                "Object id": vnic_attachment.vnic_id,
                "Public IP": vnic.public_ip,
                "MAC Address": vnic.mac_address,
                "State": vnic.lifecycle_state,
                "Private IP": vnic.private_ip,
                "Account ID": vnic.compartment_id,
                "DataCenter": region,
                "VM Object ID": vnic_attachment.instance_id,
                "VNIC Attachment ObjectID": vnic_attachment.id,
                "VNIC Attachment Name": vnic_attachment.id,
                "Tags": tags
            }, tags
        except oci.exceptions.ServiceError as e:
            print("Failed to fetch VNIC details:", e)
            return None, {}

    def fetch_vnic_attachments(compute_client, network_client, compartment_id, region):
        try:
            list_vnic_attachments_response = compute_client.list_vnic_attachments(compartment_id=compartment_id)
            vnic_details = []
            all_tags = {}
            for vnic_attachment in list_vnic_attachments_response.data:
                vnic_info, tags = fetch_vnic_details(network_client, compute_client, vnic_attachment, region)
                if vnic_info:
                    vnic_details.append(vnic_info)
                    all_tags.update(tags)
            return vnic_details, all_tags
        except oci.exceptions.ServiceError as e:
            print("Failed to fetch VNIC attachments:", e)
            return [], {}

    def db_connect_create_insert_details():
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
                table_name = "cmdb_ci_nic"
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
                    `Public IP` VARCHAR(500),
                    `MAC Address` VARCHAR(500),
                    State VARCHAR(500),
                    `Private IP` VARCHAR(500),
                    `Account ID` VARCHAR(500),
                    `DataCenter` VARCHAR(500),
                    `VM Object ID` VARCHAR(500),
                    `VNIC Attachment ObjectID` VARCHAR(500),
                    `VNIC Attachment Name` VARCHAR(500),
                    Tags JSON
                )
                """
                cursor.execute(create_table_query)
                print("Database table created successfully!")
                
                active_compartments = fetch_active_compartments(identity_client, tenancy_id)
                regions = [r.region_name for r in identity_client.list_region_subscriptions(tenancy_id=tenancy_id).data]
                details_inserted = False
                
                for region in regions:
                    compute_client = oci.core.ComputeClient(config={"region": region}, signer=signer)
                    network_client = oci.core.VirtualNetworkClient(config={"region": region}, signer=signer)
                    for compartment in active_compartments:
                        compartment_id = compartment.id
                        vnic_details, all_tags = fetch_vnic_attachments(compute_client, network_client, compartment_id, region)
                        for detail in vnic_details:
                            detail["Tags"] = json.dumps(detail["Tags"]) if detail["Tags"] else None  # Convert tags to JSON string
                            insert_query = f"""
                            INSERT INTO cmdb_ci_nic (
                                Name, `Object Id`, `Public IP`, `MAC Address`, State,
                                `Private IP`, `Account ID`, `DataCenter`, `VM Object ID`,
                                `VNIC Attachment ObjectID`, `VNIC Attachment Name`, Tags
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (
                                detail["Name"], detail["Object id"], detail["Public IP"], detail["MAC Address"], detail["State"],
                                detail["Private IP"], detail["Account ID"], detail["DataCenter"], detail["VM Object ID"],
                                detail["VNIC Attachment ObjectID"], detail["VNIC Attachment Name"], detail["Tags"]
                            ))
                            if not details_inserted:
                                details_inserted = True  # Set flag to True once details are inserted for the first time
                        connection.commit()

                if details_inserted:
                    print("Details inserted into database successfully!")
        except mysql.Error as e:
            print(f"Error: {e}")
        finally:
            connection.close()

    # if __name__ == "__main__":
    active_compartments = fetch_active_compartments(identity_client, tenancy_id)
    regions = [r.region_name for r in identity_client.list_region_subscriptions(tenancy_id=tenancy_id).data]

    for region in regions:
        compute_client = oci.core.ComputeClient(config={"region": region}, signer=signer)
        network_client = oci.core.VirtualNetworkClient(config={"region": region}, signer=signer)
        for compartment in active_compartments:
            compartment_id = compartment.id
            fetch_vnic_attachments(compute_client, network_client, compartment_id, region)

    db_connect_create_insert_details()

if __name__ == "__main__":      
    vnic_details()
    