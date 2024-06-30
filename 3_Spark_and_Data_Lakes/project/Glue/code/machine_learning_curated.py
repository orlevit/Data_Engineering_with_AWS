import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_curated
customer_curated_node1719759849993 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project3", table_name="customer_curated", transformation_ctx="customer_curated_node1719759849993")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1719759869897 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project3", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1719759869897")

# Script generated for node step_trainer_landing
step_trainer_landing_node1719759784412 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project3", table_name="step_t_landing", transformation_ctx="step_trainer_landing_node1719759784412")

# Script generated for node distinct_user
SqlQuery1 = '''
select distinct(user) from myDataSource
'''
distinct_user_node1719759979781 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":accelerometer_trusted_node1719759869897}, transformation_ctx = "distinct_user_node1719759979781")

# Script generated for node Join1
customer_curated_node1719759849993DF = customer_curated_node1719759849993.toDF()
distinct_user_node1719759979781DF = distinct_user_node1719759979781.toDF()
Join1_node1719760101648 = DynamicFrame.fromDF(customer_curated_node1719759849993DF.join(distinct_user_node1719759979781DF, (customer_curated_node1719759849993DF['email'] == distinct_user_node1719759979781DF['user']), "leftsemi"), glueContext, "Join1_node1719760101648")

# Script generated for node joining_stl_and_join1
SqlQuery0 = '''
select stl.*
from stl
join join1 on stl.serialNumber = join1.serialnumber
'''
joining_stl_and_join1_node1719760752339 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"stl":step_trainer_landing_node1719759784412, "join1":Join1_node1719760101648}, transformation_ctx = "joining_stl_and_join1_node1719760752339")

# Script generated for node ml_curated_sql
SqlQuery2 = '''
select  st_curated.*,acc_curated.user,acc_curated.x,acc_curated.y,acc_curated.z
from st_curated
join acc_curated on st_curated.sensorReadingTime = acc_curated.timestamp
'''
ml_curated_sql_node1719765936423 = sparkSqlQuery(glueContext, query = SqlQuery2, mapping = {"st_curated":joining_stl_and_join1_node1719760752339, "acc_curated":accelerometer_trusted_node1719759869897}, transformation_ctx = "ml_curated_sql_node1719765936423")

# Script generated for node S3_machine_learning_curated
S3_machine_learning_curated_node1719760991469 = glueContext.write_dynamic_frame.from_options(frame=ml_curated_sql_node1719765936423, connection_type="s3", format="json", connection_options={"path": "s3://orlevitas-s3-bucket/project3/data/machine_learning_curated/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="S3_machine_learning_curated_node1719760991469")

job.commit()