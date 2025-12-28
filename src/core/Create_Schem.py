class CreateSchema:
    @classmethod
    def create_schema(cls, spark, catalog,schema_name):
        print(schema_name)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
