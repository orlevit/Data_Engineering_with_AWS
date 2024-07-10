
import os
import json
import time
import boto3
import pandas as pd

# constants
ATHENA_DATABASE = 'udacity_project3'
ATHENA_TABLE_CUSTOMER_TRUSTED = 'customer_trusted'
ATHENA_TABLE_ACCELEROMETER_LANDING= 'accelerometer_landing'
S3_BUCKET = 'orlevitas-s3-bucket'
S3_OUTPUT_FOLDER = 'project3/data/accelerometer/trusted' 
S3_ACCELEROMETER_LANDING = 'project3/data/accelerometer/landing/'


ATHENA_S3_OUTPUT_FOLDER  = 'project3/data/accelerometer/trusted/tmp' 
S3_OUTPUT_FILENAME = 'accelerometer_trusted.json'
REGION = 'us-east-1'

QUERY = """SELECT al.*
FROM {athena_database1}.{athena_customer_trusted_table} ct
JOIN {athena_database2}.{athena_accelerometer_landing_table} al 
ON (ct.email = al.user)""".format(athena_database1=ATHENA_DATABASE, \
           athena_customer_trusted_table=ATHENA_TABLE_CUSTOMER_TRUSTED, \
           athena_database2=ATHENA_DATABASE, \
           athena_accelerometer_landing_table=ATHENA_TABLE_ACCELEROMETER_LANDING)



def run_query():
    """
    Start an Athena query execution.

    Returns:
        str: Query execution ID.
    """
    client = boto3.client('athena', region_name=REGION)
    
    response = client.start_query_execution(
        QueryString=QUERY,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': f's3://{S3_BUCKET}/{ATHENA_S3_OUTPUT_FOLDER}/'}
    )
    
    return response['QueryExecutionId']

def get_query_results(query_execution_id):
    """
    Retrieve results of an Athena query execution.

    Args:
        query_execution_id (str): The ID of the query execution.

    Returns:
        list: List of query result rows.
    
    Raises:
        Exception: If the query fails or is cancelled.
    """
    client = boto3.client('athena', region_name=REGION)
    
    # Wait for the query to complete
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5)  # Wait before checking again
    
    if status != 'SUCCEEDED':
        raise Exception(f'Query failed with status: {status}')

    # Fetch results with pagination
    results = []
    paginator = client.get_paginator('get_query_results')
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        results.extend(page['ResultSet']['Rows'])
        
    return results

def parse_results(results):
    """
    Parse Athena query results into a list of dictionaries.

    Args:
        results (list): List of raw query result rows.

    Returns:
        list: List of dictionaries containing query results.
    """
    # Extract column names from the first row (header row)
    columns = [datum.get('VarCharValue') for datum in results[0]['Data']]

    # Extract data rows
    data = []
    for row in results[1:]:  # Skip header row
        values = [datum.get('VarCharValue') for datum in row['Data']]
        data.append(dict(zip(columns, values)))

    return data

def save_to_s3(data):
    """
    Save the data to an S3 bucket.

    Args:
        data (str): The data to save (in string format).
    """
    s3_client = boto3.client('s3', region_name=REGION)
    
    # Save to local file
    with open(S3_OUTPUT_FILENAME, 'w') as f:
        f.write(data)

    # Upload to S3
    s3_client.upload_file(S3_OUTPUT_FILENAME, S3_BUCKET, f'{S3_OUTPUT_FOLDER}/{S3_OUTPUT_FILENAME}')
    print(f'File uploaded to s3://{S3_BUCKET}/{S3_OUTPUT_FOLDER}/{S3_OUTPUT_FILENAME}')

def delete_s3_objects(bucket_name, prefix):
    """
    Delete all objects under a prefix in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix path to delete objects under.
    """
    s3_client = boto3.client('s3', region_name=REGION)
    paginator = s3_client.get_paginator('list_objects_v2')
    delete_requests = {'Objects': []}

    # Paginate through S3 objects and add delete requests to batch
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                delete_requests['Objects'].append({'Key': obj['Key']})

        # Delete objects in batches of up to 1000
        if len(delete_requests['Objects']) >= 1000:
            s3_client.delete_objects(Bucket=bucket_name, Delete=delete_requests)
            delete_requests = {'Objects': []}

    # Delete any remaining objects
    if delete_requests['Objects']:
        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_requests)

    # Delete the prefix itself
    s3_client.delete_object(Bucket=bucket_name, Key=prefix)

def convert_json_to_string(data):
    """
    Convert a list of dictionaries to a newline-delimited JSON string.

    Args:
        data (list): The data to convert.

    Returns:
        str: The converted JSON string.
    """
    def filter_non_null_fields(data_dict):
        return {key: value for key, value in data_dict.items() if value is not None}
    
    json_results_dict = []
    converted_string = ''
    
    for line in data:
        filted_dict = filter_non_null_fields(line)
        json_string = json.dumps(filted_dict)
        converted_string += json_string + '\n'
        
    return converted_string





query_execution_id = run_query()
print(f'Query execution ID: {query_execution_id}')

results = get_query_results(query_execution_id)
print('Query execution completed.')

data = parse_results(results)
print('Results parsed.')

converted_string = convert_json_to_string(data)
print('converted json to string')

save_to_s3(converted_string)
print('Results saved to S3.')

delete_s3_objects(S3_BUCKET, ATHENA_S3_OUTPUT_FOLDER)
print('Deleted temporary files from athena query to S3.')

# Clean up local file
if os.path.exists(S3_OUTPUT_FILENAME):
    os.remove(S3_OUTPUT_FILENAME)

print('Finished')







