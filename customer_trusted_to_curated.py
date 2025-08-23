# STEDI Human Balance Analytics - Customer Trusted to Curated ETL Job
# Purpose: Create curated dataset with customers who consented AND have accelerometer data
# Business Rule: Customers must both give consent AND actively use the device

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

# Read trusted customer data (customers who consented to research)
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Read trusted accelerometer data (privacy-compliant sensor readings)
AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# Create curated customer dataset - only customers with both consent AND accelerometer activity
CustomerCuratedJoin_node3 = sparkSqlQuery(
    glueContext,
    query="""SELECT DISTINCT 
                c.customerName, c.email, c.phone, c.birthDay, c.serialNumber, 
                c.registrationDate, c.lastUpdateDate, c.shareWithResearchAsOfDate, 
                c.shareWithPublicAsOfDate, c.shareWithFriendsAsOfDate 
             FROM customer c 
             JOIN accelerometer a ON c.email = a.user""",
    mapping={"customer": CustomerTrusted_node1, "accelerometer": AccelerometerTrusted_node2},
    transformation_ctx="CustomerCuratedJoin_node3",
)

# Write curated customer data to curated zone
CustomerCurated_node4 = glueContext.getSink(
    path="s3://stedi-damiano-d609/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node4",
)
CustomerCurated_node4.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node4.setFormat("json")
CustomerCurated_node4.writeFrame(CustomerCuratedJoin_node3)

job.commit()