class CreateTable:
    @classmethod
    def create_table(cls, config):
        # Get the Spark session from the config
        spark = config["spark"]
        
       # Check if the table already exists
        table_exist = spark.catalog.tableExists(f"{config['catalog']}.{config['schema']}.{config['table_name']}")
        if table_exist:
            print(f"Table {config['catalog']}.{config['schema']}.{config['table_name']} already exists")
            return

        # Create the SQL based on table type
        if config["table_type"] == "Managed":
            sql = f"""
            CREATE TABLE {config['catalog']}.{config['schema']}.{config['table_name']}
            """
        else:
            sql = f"""
            CREATE TABLE {config['catalog']}.{config['schema']}.{config['table_name']}
             USING DELTA LOCATION '{config['path']}'
            """
        
        # Execute the SQL to create the table
        spark.sql(sql)
        print(f"Table {config['catalog']}.{config['schema']}.{config['table_name']} created")
