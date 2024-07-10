import os
import json
import time
import boto3
import pandas as pd

# constants
ATHENA_DATABASE = 'udacity_project3'
ATHENA_TABLE = 'customer_landing'
S3_BUCKET = 'orlevitas-s3-bucket'
S3_OUTPUT_FOLDER = 'project3/data/customer/trusted' 
ATHENA_S3_OUTPUT_FOLDER  = 'project3/data/customer/trusted/tmp' 
S3_OUTPUT_FILENAME = 'customer_trusted.json'
REGION = 'us-east-1'
QUERY = """SELECT *
FROM {athena_database}.{athena_table} 
WHERE sharewithresearchasofdate IS NOT NULL
""".format(athena_database=ATHENA_DATABASE, athena_table=ATHENA_TABLE)





def run_query():
    """
    Start an Athena query execution.

    Initializes a Boto3 Athena client and starts a query execution with the specified
    query string, database, and output location.

    Returns:
        str: The ID of the query execution.
    """
    client = boto3.client('athena', region_name=REGION)
    
    # Start query execution
    response = client.start_query_execution(
        QueryString=QUERY,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': f's3://{S3_BUCKET}/{ATHENA_S3_OUTPUT_FOLDER}/'}
    )
    return response['QueryExecutionId']

def get_query_results(query_execution_id):
    """
    Retrieve the results of an Athena query.

    Waits for the query to complete, then fetches the results if the query succeeded.
    Raises an exception if the query failed.

    Args:
        query_execution_id (str): The ID of the query execution.

    Returns:
        dict: The results of the query.
    """
    client = boto3.client('athena', region_name=REGION)
    
    # Wait for the query to complete
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5)  # Wait before checking the status again
    
    if status != 'SUCCEEDED':
        raise Exception(f'Query failed with status: {status}')
    
    # Retrieve the query results
    results = client.get_query_results(QueryExecutionId=query_execution_id)
    return results

def parse_results(results):
    """
    Parse the results of an Athena query into a list of dictionaries.

    Extracts column labels and row data, then combines them into a list of dictionaries
    where each dictionary represents a row with column names as keys.

    Args:
        results (dict): The raw results of the query.

    Returns:
        list[dict]: Parsed results with each dictionary representing a row.
    """
    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = results['ResultSet']['Rows'][1:]  # Skip header row
    data = []

    for row in rows:
        values = [datum.get('VarCharValue', None) for datum in row['Data']]
        data.append(dict(zip(columns, values)))

    return data

def save_to_s3(data):
    """
    Save data to a local file and upload it to an S3 bucket.

    Writes the data to a local file in JSON format, then uploads the file to
    a specified location in S3.

    Args:
        data (list[dict]): The data to be saved.
    """
    s3_client = boto3.client('s3', region_name=REGION)
    
    # Save data to a local file
    with open(S3_OUTPUT_FILENAME, 'w') as f:
        # Convert data to JSON string and write to file
        json.dump(data, f)

    # Upload the local file to S3
    s3_client.upload_file(S3_OUTPUT_FILENAME, S3_BUCKET, f'{S3_OUTPUT_FOLDER}/{S3_OUTPUT_FILENAME}')
    print(f'File uploaded to s3://{S3_BUCKET}/{S3_OUTPUT_FOLDER}/{S3_OUTPUT_FILENAME}')

def delete_s3_objects(bucket_name, prefix):
    """
    Delete all objects under a prefix in an S3 bucket.

    Iterates through all objects under the specified prefix and deletes them in batches.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix under which to delete objects.
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
    Convert a list of dictionaries to a newline-separated JSON string.

    Filters out fields with None values and converts each dictionary to a JSON string.

    Args:
        data (list[dict]): The data to convert.

    Returns:
        str: A newline-separated JSON string representing the data.
    """
    def filter_non_null_fields(data_dict):
        # Remove keys with None values
        return {key: value for key, value in data_dict.items() if value is not None}
    
    json_results_dict = []
    converted_string = ''
    
    for line in data:
        filtered_dict = filter_non_null_fields(line)
        json_string = json.dumps(filtered_dict)
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

