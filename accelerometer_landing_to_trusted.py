# STEDI Human Balance Analytics - Accelerometer Landing to Trusted ETL Job
# Purpose: Join accelerometer data with trusted customers for privacy compliance
# Only keep accelerometer data where user email matches a consented customer

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read all accelerometer data from landing zone
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Read trusted customer data (customers who consented to research)
CustomerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node2",
)

# Join accelerometer data with trusted customers - privacy filter
PrivacyJoin_node3 = sparkSqlQuery(
    glueContext,
    query="SELECT a.user, a.timestamp, a.x, a.y, a.z FROM accelerometer a JOIN customer c ON a.user = c.email",
    mapping={"accelerometer": AccelerometerLanding_node1, "customer": CustomerTrusted_node2},
    transformation_ctx="PrivacyJoin_node3",
)

# Write privacy-compliant accelerometer data to trusted zone
AccelerometerTrusted_node4 = glueContext.getSink(
    path="s3://stedi-damiano-d609/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node4",
)
AccelerometerTrusted_node4.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node4.setFormat("json")
AccelerometerTrusted_node4.writeFrame(PrivacyJoin_node3)

job.commit()
