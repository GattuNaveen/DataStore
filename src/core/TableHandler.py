class TableHandler:

    @classmethod
    def write(cls, spark, df, mode: str = "overwrite", format: str = "delta", table_name: str = "", path: str = "", mergeSchema: bool = False, partitions: list = []):
        write_options = df.write.format(format).mode(mode)
        
        if mergeSchema:
            write_options.option("mergeSchema", mergeSchema)
        
        if partitions:
            write_options.partitionBy(partitions)
        
        if table_name:
            write_options.saveAsTable(table_name)
            print(f"Data written to {table_name} successfully")
        elif path: 
            write_options.save(path)
            print(f"Data written to {path} successfully")
        else:
            raise ValueError("Either table_name or path must be provided")

    @classmethod
    def upsert(cls, spark, source_df, table_name, key_columns: list, exclude_columns: list):
        target_table = DeltaTable.forName(spark, table_name)

        source_df = source_df.alias("source")
        target_table = target_table.alias("target")

        # Columns to update (exclude keys and explicitly excluded columns)
        update_cols = [
            c for c in source_df.columns
            if c not in key_columns and c not in exclude_columns
        ]

        # Merge condition
        merge_key = " AND ".join(
            [f"target.{c} = source.{c}" for c in key_columns]
        )

        # Update and insert mappings
        update_set = {c: f"source.{c}" for c in update_cols}
        insert_set = {c: f"source.{c}" for c in source_df.columns}

        (
            target_table
            .merge(source_df, merge_key)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )
