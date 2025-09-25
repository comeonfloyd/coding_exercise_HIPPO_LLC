"""
This module defines constants used throughout the project, 
Spark schemas for DataFrame constructions, and file paths for data storage and results output. Correct!!!
"""


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    TimestampType,
)

# Paths
PHARMACY_DIR = "./data/pharmacies"
CLAIMS_DIR = "./data/claims"
REVERTS_DIR = "./data/reverts"

TASK_1_2_RESULT = "results/task_1_2_result.json"
TASK_3_RESULT = "results/task_3_result.json"
TASK_4_RESULT = "results/task_4_result.json"


PHARMACIES_REQUIRED_FIELDS = ['chain', 'npi']
CLAIMS_REQUIRED_FIELDS = ['id', 'npi', 'ndc', 'price', 'quantity', 'timestamp']
REVERTS_REQUIRED_FIELDS = ['id', 'claim_id', 'timestamp']


# Spark Schemas
PHARMACIES_SCHEMA = StructType(
    [
        StructField("chain", StringType(), True), 
        StructField("npi", StringType(), True)
    ]
)

CLAIMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("npi", StringType(), True),
        StructField("ndc", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("quantity", FloatType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

