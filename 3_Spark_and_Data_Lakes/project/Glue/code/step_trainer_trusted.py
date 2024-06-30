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

# Script generated for node step_trainer_landing
step_trainer_landing_node1719759784412 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project3", table_name="step_t_landing", transformation_ctx="step_trainer_landing_node1719759784412")

# Script generated for node customer_curated
customer_curated_node1719759849993 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project3", table_name="customer_curated", transformation_ctx="customer_curated_node1719759849993")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1719759869897 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project3", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1719759869897")

# Script generated for node distinct_user
SqlQuery0 = '''
select distinct(user) from myDataSource
'''
distinct_user_node1719759979781 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":accelerometer_trusted_node1719759869897}, transformation_ctx = "distinct_user_node1719759979781")

# Script generated for node Join1
customer_curated_node1719759849993DF = customer_curated_node1719759849993.toDF()
distinct_user_node1719759979781DF = distinct_user_node1719759979781.toDF()
Join1_node1719760101648 = DynamicFrame.fromDF(customer_curated_node1719759849993DF.join(distinct_user_node1719759979781DF, (customer_curated_node1719759849993DF['email'] == distinct_user_node1719759979781DF['user']), "leftsemi"), glueContext, "Join1_node1719760101648")

# Script generated for node joining_stl_and_join1
SqlQuery1 = '''
select * 
from stl
join join1 on stl.serialNumber = join1.serialnumber
'''
joining_stl_and_join1_node1719760752339 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"stl":step_trainer_landing_node1719759784412, "join1":Join1_node1719760101648}, transformation_ctx = "joining_stl_and_join1_node1719760752339")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1719760991469 = glueContext.write_dynamic_frame.from_options(frame=joining_stl_and_join1_node1719760752339, connection_type="s3", format="json", connection_options={"path": "s3://orlevitas-s3-bucket/project3/data/step_trainer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="step_trainer_trusted_node1719760991469")

job.commit()