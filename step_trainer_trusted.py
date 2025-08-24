# STEDI Human Balance Analytics - Step Trainer Landing to Trusted ETL Job
# Purpose: Filter step trainer device readings to only include data from curated customers
# Quality Rule: Only keep device data from customers who consented AND actively use accelerometer

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

# Read all step trainer device data from S3 bucket
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-damiano-d609/step_trainer/landing/"]},
    transformation_ctx="StepTrainerLanding_node1",
)

# Read curated customer data (customers with consent AND accelerometer activity)
CustomerCurated_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node2",
)

# Join step trainer data with curated customers - quality filter
StepTrainerPrivacyJoin_node3 = sparkSqlQuery(
    glueContext,
    query="""SELECT DISTINCT s.sensorReadingTime, s.serialNumber, s.distanceFromObject 
             FROM step_trainer s 
             JOIN customer c ON s.serialNumber = c.serialNumber""",
    mapping={"step_trainer": StepTrainerLanding_node1, "customer": CustomerCurated_node2},
    transformation_ctx="StepTrainerPrivacyJoin_node3",
)

# Write quality-filtered step trainer data to trusted zone
StepTrainerTrusted_node4 = glueContext.getSink(
    path="s3://stedi-damiano-d609/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node4",
)
StepTrainerTrusted_node4.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node4.setFormat("json")
StepTrainerTrusted_node4.writeFrame(StepTrainerPrivacyJoin_node3)

job.commit()
