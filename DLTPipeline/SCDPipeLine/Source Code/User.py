import dlt
from pyspark.sql.functions import col, current_timestamp

# Constants ...
CUSTOMER_KEYS = ["ExternalId"]
SCD_COLUMNS = ["UserName","LegalName","Email","ManagerId","BusinessunitId","JobTitle"]

def user_expectations():
    return {
        "Valid_User_ExternalId": "Id IS NOT NULL",
        "Valid_email": "email LIKE '%@%'"
    }

def user_transformation(df):
    return (
        df
        .withColumn("InsertedAt", current_timestamp())
        .select(
            col("id").alias("ExternalId"),
            col("name").alias("UserName"),
            col("preferredName").alias("LegalName"),
            col("email").alias("Email"),
            col("title").alias("JobTitle"),
            col("division.id").alias("BusinessunitId"),
            col("manager.id").alias("ManagerId"),
            col("InsertedAt")
        )
    )

@dlt.table(name="bronze_user")
@dlt.expect_all(user_expectations())
def bronze_user():
    # registers this dataset as part of the pipeline
    return spark.readStream.table("datastore.bronze.user")

@dlt.table(name="silver_user")
def silver_user():
    return user_transformation(dlt.read_stream("bronze_user"))

dlt.create_streaming_table(name="gold_user")

dlt.create_auto_cdc_flow(
    target="gold_user",
    source="silver_user",
    keys=CUSTOMER_KEYS,
    sequence_by=col("InsertedAt"),
    stored_as_scd_type=2
)
