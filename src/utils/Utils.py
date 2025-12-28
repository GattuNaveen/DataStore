from pyspark.sql.functions import sha2, col, lit, concat_ws, coalesce, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql.types import TimestampType

class Utils:
    @classmethod
    def hash_columns(cls,columns):
        hash_coulmns = sha2(
            concat_ws(
                *[coalesce(col(c).cast("string"), lit("")) for c in columns]
            ),
            256
        )
        print("hash key generated sucessfully")
        return hash_coulmns
    
    @classmethod
    def scdType2(cls,spark, source_df, target_table, key: list):
        target_dt = DeltaTable.forName(spark, target_table)

        merge_key = " AND ".join([f"source.{c} = target.{c}" for c in key])

        target_dt.alias("target") \
            .merge(
                source_df.alias("source"),
                merge_key
            ) \
            .whenMatchedUpdate(
                condition="target.IsActive = true",
                set={
                    "UpdatedAt": current_timestamp(),
                    "IsActive": lit(False)
                }
            ) \
            .execute()

        source_df \
            .withColumn("UpdatedAt", lit(None).cast(TimestampType())) \
            .withColumn("IsActive", lit(True)) \
            .write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)

