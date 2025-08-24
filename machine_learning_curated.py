# STEDI Human Balance Analytics - Machine Learning Curated Dataset ETL Job
# Purpose: Create final ML dataset by synchronizing step trainer and accelerometer readings
# ML Requirement: Match device readings with accelerometer data at same timestamp

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

# Read trusted step trainer data
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Read trusted accelerometer data
AccelerometerTrusted_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node2",
)

# Read curated customer data for referential integrity
CustomerCurated_node3 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node3",
)

# Create ML dataset with synchronized sensor readings (3-way join by timestamp and serial number)
MachineLearningJoin_node4 = sparkSqlQuery(
    glueContext,
    query="""SELECT 
                s.sensorReadingTime, s.serialNumber, s.distanceFromObject,
                a.user, a.x, a.y, a.z
             FROM step_trainer s 
             JOIN accelerometer a ON s.sensorReadingTime = a.timestamp
             JOIN customer c ON s.serialNumber = c.serialNumber""",
    mapping={
        "step_trainer": StepTrainerTrusted_node1, 
        "accelerometer": AccelerometerTrusted_node2,
        "customer": CustomerCurated_node3
    },
    transformation_ctx="MachineLearningJoin_node4",
)

# Write final ML dataset to curated zone
MachineLearningCurated_node5 = glueContext.getSink(
    path="s3://stedi-damiano-d609/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node5",
)
MachineLearningCurated_node5.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node5.setFormat("json")
MachineLearningCurated_node5.writeFrame(MachineLearningJoin_node4)

job.commit()