print('Program Started....')
import psycopg2 
import pandas as pd
from datetime import datetime
import warnings
from warnings import filterwarnings
import boto3 # type: ignore
import botocore.exceptions
from urllib.parse import urlparse
import os
import sys
import subprocess

from dotenv import load_dotenv

# ✅ Ensure we always load latest .env values
env_path = os.environ.get("DOTENV_PATH", os.path.join(os.getcwd(), ".env"))

if os.environ.get("USE_UPDATED_ENV") == "1":
    load_dotenv(dotenv_path=env_path, override=True)
else:
    load_dotenv(dotenv_path=env_path)


DEFAULT_AWS_PROFILE = os.getenv('AWS_PROFILE')


def is_sso_login_required(profile: str = None) -> bool:
    if profile is None:
        profile = DEFAULT_AWS_PROFILE
    try:
        session = boto3.Session(profile_name=profile)
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        # print(f"✅ AWS credentials valid for ARN: {identity['Arn']}")
        return False
    except botocore.exceptions.ClientError as e:
        print(f"❌ Invalid AWS credentials: {e}")
        return True
    except botocore.exceptions.BotoCoreError as e:
        print(f"❌ BotoCore error: {e}")
        return True

def trigger_sso_login(profile: str = None):
    if profile is None:
        profile = DEFAULT_AWS_PROFILE
    print(f"🔐 Running AWS SSO login for profile '{profile}'...")
    try:
        result = subprocess.run(
            ['aws', 'sso', 'login', '--profile', profile],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"✅ AWS SSO login successful.\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"❌ SSO login failed:\n{e.stderr}")
        sys.exit(1)
    except FileNotFoundError:
        print("❌ AWS CLI not found. Make sure it is installed and in your PATH.")
        sys.exit(1)


# Check if SSO login is needed
def run_pipeline(logger=print):
    if is_sso_login_required(os.getenv('AWS_PROFILE')):
        logger(f"🔐 Running AWS SSO login for profile '{os.getenv('AWS_PROFILE')}'...")
        try:
            result = subprocess.run(
                ['aws', 'sso', 'login', '--profile', os.getenv('AWS_PROFILE')],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            logger(f"✅ AWS SSO login successful.\n{result.stdout}")
        except subprocess.CalledProcessError as e:
            logger(f"❌ SSO login failed:\n{e.stderr}")
            return
        except FileNotFoundError:
            logger("❌ AWS CLI not found. Make sure it is installed and in your PATH.")
            return

    # Suppress the specific UserWarning from pandas, to ignore psycopg2 warnings
    warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy connectable.*", module="pandas")

    s3_paths = os.getenv('S3_LOCATION')
    if not s3_paths:
        logger("❌ S3_LOCATION environment variable is missing.")
        return

    s3_paths_list = s3_paths.split(',')
    temp_db = ''
    fnl_schema_list = []
    
    conn = None
    cursor = None

    try:
        for s3_path in sorted(s3_paths_list):
            logger(f'Starting import from the "{s3_path}"')
             
            filepath = s3_path.strip()
            # Basic validation
            if 's3://' in filepath:
                parts = filepath.replace('s3://', '').split('/')
                if len(parts) > 4:
                    schema = parts[4]
                else:
                    logger(f"❌ Invalid S3 Path format: {filepath}")
                    continue
            else:
                 logger(f"❌ Invalid S3 Path (must start with s3://): {filepath}")
                 continue

            dbname=schema[0].lower()
            user=os.getenv('REDSHIFT_USER')
            password=os.getenv('REDSHIFT_PASSWORD')
            host=os.getenv('REDSHIFT_HOST')
            port=os.getenv('REDSHIFT_PORT')

            
            if temp_db == '' or temp_db != dbname:
                if conn:
                    cursor.close()
                    conn.close()
                    logger("Previous database connection closed.")
                
                print(f'Connecting to "{dbname}" database') # keep print for stdout debug
                logger(f'Connecting to "{dbname}" database')
                # Connect to Redshift
                conn = psycopg2.connect(
                    dbname=dbname.lower(),
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )
                conn.autocommit = True  # Automatically commit changes
                cursor = conn.cursor()
                logger("Database connection successful!")

            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            today = datetime.now().strftime('%Y%m%d')
            filterwarnings("ignore", category=UserWarning, message='.*pandas only supports SQLAlchemy connectable.*')

            sch_nm = f'{schema}_sls_{today}'
            fnl_schema_list.append(sch_nm)

            def segregate_s3_uri(filepath):
                # Parse the S3 URI using urlparse
                parsed_uri = urlparse(filepath)
                
                # Extract bucket and prefix
                bucket_name = parsed_uri.netloc
                prefix = parsed_uri.path.lstrip('/')
                
                return bucket_name, prefix


            bucket_name, prefix = segregate_s3_uri(filepath)


            # Initialize the S3 client
            profile = os.getenv('AWS_PROFILE')
            session = boto3.Session(profile_name=profile)
            s3 = session.client('s3')


            def list_folders_with_csv_in_name(bucket_name, parent_prefix):
                """List all subfolders inside a specific S3 folder where the folder name contains '.csv'."""
                folders_with_csv_in_name = set()  # Using a set to avoid duplicates
                
                # List the objects in the given S3 bucket and folder (prefix), with delimiter to get folder names
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=parent_prefix, Delimiter='/')
                
                # Loop through each common prefix (subfolder)
                for prefix in response.get('CommonPrefixes', []):
                    folder_name = prefix['Prefix'].rstrip('/')  # Remove the trailing slash
                    
                    # Check if '.csv' is in the folder name
                    if '.csv' in folder_name:
                        # Get only the last part of the folder name (e.g., folder2 from your-folder/folder2.csv)
                        folder_last_part = folder_name.split('/')[-1]
                     
                        folders_with_csv_in_name.add(folder_last_part)
                       
                return list(folders_with_csv_in_name)
            
            
            def list_folders_stage1(bucket_name, parent_prefix):
                folders_name = set()  # Using a set to avoid duplicates
                
                # List the objects in the given S3 bucket and folder (prefix), with delimiter to get folder names
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=parent_prefix, Delimiter='/')
                
                # Loop through each common prefix (subfolder)
                for prefix in response.get('CommonPrefixes', []):
                    folder_name = prefix['Prefix'].rstrip('/')  # Remove the trailing slash
                    folder_last_part = folder_name.split('/')[-1]
                    if folder_last_part.startswith('lab'):
                        folder_last_part = 'lab'
                    folders_name.add(folder_last_part)
                              
                return list(folders_name)
            

            if 'stage1' in s3_path:
                folders = list_folders_stage1(bucket_name, prefix)
            else:
                folders = list_folders_with_csv_in_name(bucket_name, prefix)

            

            dataset_list =  [folder.replace(".csv", "") for folder in folders]
            if not dataset_list:
                logger('There is no files in the given directory.')
            else:
                logger(f'Datasets available are: {dataset_list}')
                logger("")

                # Creating schema
                external_schema = f'''create external schema if not exists {sch_nm}_external from data catalog  
                database '{schema}' iam_role 'arn:aws:iam::985867512284:role/rol_data_infra_spectrum01'
                create external database if not exists'''
                cursor.execute(external_schema)

                schema_qry = f'''create schema if not exists {schema}_sls_{today} '''
                cursor.execute(schema_qry)
                
                # Calling import function and building queries for datasets
                if 'stage1' in s3_path:
                    queries_set = [ f'''select perm_stage1_{dataset}_530('{today}','{schema}_sls','{filepath}{dataset}')''' for dataset in dataset_list]
                    # Dropping table if exists
                    for dataset in dataset_list:
                        cursor.execute(f'''drop table if exists {sch_nm}_external.perm_stage1_{dataset}''')  

                    i = 0

                    for query in queries_set:
                        try:
                            cursor.execute(query)
                            result = cursor.fetchall()
                            create_external_query = str(result[0][0]).split(";")[0]  # Create external table query
                            cursor.execute(create_external_query)
                            logger(f"External table created: {sch_nm}.perm_stage1_{dataset_list[i]}_external")
                            create_view_query = str(result[0][0]).split(";")[1]  # Create view query
                            cursor.execute(create_view_query)
                            logger(f"View created: {sch_nm}.perm_stage1_{dataset_list[i]}")
                            i += 1

                            def grant_privileges(cursor, sch_nm):
                                # Grant all on schema
                                cursor.execute(f'GRANT ALL ON SCHEMA {sch_nm}_external TO PUBLIC')
                                cursor.execute(f'GRANT ALL ON SCHEMA {sch_nm} TO PUBLIC')

                                # Grant select, insert, update, delete on all tables in schema
                                cursor.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {sch_nm}_external TO PUBLIC')
                                cursor.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {sch_nm} TO PUBLIC')

                                # Grant privileges on all tables in schema (both current and future)
                                cursor.execute(f"""
                                    SELECT table_name 
                                    FROM information_schema.tables
                                    WHERE table_schema = '{sch_nm}_EXTERNAL' AND table_type = 'BASE TABLE'
                                """)
                                tables_external = cursor.fetchall()
                                for table in tables_external:
                                    cursor.execute(f'GRANT ALL PRIVILEGES ON TABLE {sch_nm}_external.{table[0]} TO PUBLIC')

                                cursor.execute(f"""
                                    SELECT table_name 
                                    FROM information_schema.tables
                                    WHERE table_schema = '{sch_nm}' AND table_type = 'BASE TABLE'
                                """)
                                tables = cursor.fetchall()
                                for table in tables:
                                    cursor.execute(f'GRANT ALL PRIVILEGES ON TABLE {sch_nm}.{table[0]} TO PUBLIC')

                                # Grant privileges on all views in schema
                                cursor.execute(f"""
                                    SELECT table_name 
                                    FROM information_schema.views
                                    WHERE table_schema = '{sch_nm}_EXTERNAL'
                                """)
                                views_external = cursor.fetchall()
                                for view in views_external:
                                    cursor.execute(f'GRANT ALL PRIVILEGES ON VIEW {sch_nm}_external.{view[0]} TO PUBLIC')

                                cursor.execute(f"""
                                    SELECT table_name 
                                    FROM information_schema.views
                                    WHERE table_schema = '{sch_nm}'
                                """)
                                views = cursor.fetchall()
                                for view in views:
                                    cursor.execute(f'GRANT ALL PRIVILEGES ON VIEW {sch_nm}.{view[0]} TO PUBLIC')


                            grant_privileges(cursor, sch_nm)

                        except Exception as query_error:
                            logger(f"Error executing query for dataset {dataset_list[i]}: {query_error}")

                    
                else:                
                    queries_set = [ f'''select perm_stage_{dataset}_530('{today}','{schema}_sls','{filepath}{dataset}.csv')''' for dataset in dataset_list]
                    for dataset in dataset_list:
                        cursor.execute(f'''drop table if exists {sch_nm}_external.perm_stage_{dataset}''') 
                    # Incremental value for dataset info
                    i = 0
                    for query in queries_set:
                        try:
                            cursor.execute(query)
                            result = cursor.fetchall()
                            create_external_query = str(result[0][0]).split(";")[0]  # Create external table query
                            cursor.execute(create_external_query)
                            logger(f"External table created: {sch_nm}.perm_stage_{dataset_list[i]}_external")
                            create_view_query = str(result[0][0]).split(";")[1]  # Create view query
                            cursor.execute(create_view_query)
                            logger(f"View created: {sch_nm}.perm_stage_{dataset_list[i]}")
                            i += 1

                            def grant_privileges(cursor, sch_nm):
                                # Grant all on schema
                                cursor.execute(f'GRANT ALL ON SCHEMA {sch_nm}_external TO PUBLIC')
                                cursor.execute(f'GRANT ALL ON SCHEMA {sch_nm} TO PUBLIC')

                                # Grant select, insert, update, delete on all tables in schema
                                cursor.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {sch_nm}_external TO PUBLIC')
                                cursor.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {sch_nm} TO PUBLIC')

                                # Grant privileges on all tables in schema (both current and future)
                                cursor.execute(f"""
                                    SELECT table_name 
                                    FROM information_schema.tables
                                    WHERE table_schema = '{sch_nm}_EXTERNAL' AND table_type = 'BASE TABLE'
                                """)
                                tables_external = cursor.fetchall()
                                for table in tables_external:
                                    cursor.execute(f'GRANT ALL PRIVILEGES ON TABLE {sch_nm}_external.{table[0]} TO PUBLIC')

                                cursor.execute(f"""
                                    SELECT table_name 
                                    FROM information_schema.tables
                                    WHERE table_schema = '{sch_nm}' AND table_type = 'BASE TABLE'
                                """)
                                tables = cursor.fetchall()
                                for table in tables:
                                    cursor.execute(f'GRANT ALL PRIVILEGES ON TABLE {sch_nm}.{table[0]} TO PUBLIC')

                                # Grant privileges on all views in schema
                                cursor.execute(f"""
                                    SELECT table_name 
                                    FROM information_schema.views
                                    WHERE table_schema = '{sch_nm}_EXTERNAL'
                                """)
                                views_external = cursor.fetchall()
                                for view in views_external:
                                    cursor.execute(f'GRANT ALL PRIVILEGES ON VIEW {sch_nm}_external.{view[0]} TO PUBLIC')

                                cursor.execute(f"""
                                    SELECT table_name 
                                    FROM information_schema.views
                                    WHERE table_schema = '{sch_nm}'
                                """)
                                views = cursor.fetchall()
                                for view in views:
                                    cursor.execute(f'GRANT ALL PRIVILEGES ON VIEW {sch_nm}.{view[0]} TO PUBLIC')


                            grant_privileges(cursor, sch_nm)

                        except Exception as query_error:
                            logger(f"Error executing query for dataset {dataset_list[i]}: {query_error}")

                

                logger('Tables have been imported successfully')
                logger(f'Schema info: {sch_nm}')
            
            temp_db = dbname
            logger("--------------------------------------------------")

    except psycopg2.Error as e:
        logger('Database not connected. Please check your redshift credentials and try again.')
        logger(f"Error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        logger(f"An error occurred: {e}")
        import traceback
        logger(traceback.format_exc())
    finally: 
        if conn:
            cursor.close()
            conn.close()
            logger("Database connection closed.")
        
        logger(f'The final schema : {fnl_schema_list}') 

if __name__ == "__main__":
    run_pipeline() 



