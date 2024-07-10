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

# Script generated for node customer_trusted
customer_trusted_node1729259348911 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project3", table_name="customer_trusted", transformation_ctx="customer_trusted_node1729259348911")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1729729429822 = glueContext.create_dynamic_frame.from_catalog(database="udacity_project3", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1729729429822")

# Script generated for node distinct_user
SqlQuery0 = '''
select distinct(user) from myDataSource
'''
distinct_user_node1724750669384 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":accelerometer_trusted_node1729729429822}, transformation_ctx = "distinct_user_node1724750669384")

# Script generated for node Join
customer_trusted_node1728711849493DF = customer_trusted_node1729259348911.toDF()
distinct_user_node1720750989782DF = distinct_user_node1724750669384.toDF()
#Join_node1722215101219 = DynamicFrame.fromDF(customer_curated_node1728711849493DF.join(distinct_user_node1720750989782DF, customer_trusted_node1728711849493DF['email'] == distinct_user_node1720750989782DF['user']), "inner"), glueContext, "Join1_node1719760101648")

# Script generated for node joining_stl_and_join1
SqlQuery1 = '''
select customer_trusted.* 
from customer_trusted
inner join on customer_trusted.email = acc_trusted.user
'''
joining_node1724461552130 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"customer_trusted":customer_trusted_node1728711849493DF, "acc_trusted":distinct_user_node1720750989782DF}, transformation_ctx = "joining_node1724461552130")

# Script generated for node customer_curated
customer_curated_node1723737781140 = glueContext.write_dynamic_frame.from_options(frame=joining_node1724461552130, connection_type="s3", format="json", connection_options={"path": "s3://orlevitas-s3-bucket/project3/data/customer/curated/", "compression": "snappy", "partitionKeys": [], "enableUpdateCatalog": True}, transformation_ctx="customer_curated_node1723737781140")

job.commit()
