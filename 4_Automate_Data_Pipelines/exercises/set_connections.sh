# update the following bucket name to match the name of your S3 bucket and un-comment it:
#
## airflow variables set s3_bucket sean-murdock
airflow variables set s3_bucket orlevitas
#
## TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines

#[{"id": "61", "conn_id": "redshift", "conn_type": "redshift", "description": "", "host": "default-workgroup.876193940005.us-east-1.redshift-serverless.amazonaws.com", "schema": "dev", "login": "awsuser", "password": "R3dsh1ft", "port": "5439", "is_encrypted": "False", "is_extra_encrypted": "False", "extra_dejson": {}, "get_uri": "redshift://awsuser:R3dsh1ft@default-workgroup.876193940005.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
