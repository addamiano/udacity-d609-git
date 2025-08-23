# STEDI Human Balance Analytics - Customer Landing to Trusted ETL Job
# Purpose: Filter customer data to only include customers who consented to research
# Privacy Rule: Only include customers where shareWithResearchAsOfDate IS NOT NULL

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Helper function to run SQL queries on Glue DynamicFrames
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Initialize Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read all customer data from landing table
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1",
)

# Apply privacy filter - keep only customers who consented to research
PrivacyFilter_node2 = sparkSqlQuery(
    glueContext,
    query="SELECT * FROM myDataSource WHERE shareWithResearchAsOfDate IS NOT NULL",
    mapping={"myDataSource": CustomerLanding_node1},
    transformation_ctx="PrivacyFilter_node2",
)

# Write filtered customer data to trusted zone
CustomerTrusted_node3 = glueContext.getSink(
    path="s3://stedi-damiano-d609/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node3",
)
CustomerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node3.setFormat("json")
CustomerTrusted_node3.writeFrame(PrivacyFilter_node2)

job.commit()