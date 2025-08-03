import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType


def string_to_iso_date(df: DataFrame, string_col: str, new_col: str) -> DataFrame:
    def format_timestamp(col_expr):
        return F.to_timestamp(
            F.regexp_replace(col_expr, r"\.\d+", ""), "yyyy-MM-dd'T'HH:mm:ss"
        )

    schema_field = next(field for field in df.schema.fields if field.name == string_col)

    if isinstance(schema_field.dataType, ArrayType):
        if isinstance(schema_field.dataType.elementType, ArrayType):
            transformation = F.transform(
                F.col(string_col),
                lambda outer_array: F.transform(outer_array, format_timestamp),
            )
        else:
            transformation = F.transform(F.col(string_col), format_timestamp)
    else:
        transformation = format_timestamp(F.col(string_col))

    return df.withColumn(new_col, transformation)


def generate_hash(df: DataFrame, hash_col_name: str, kind: str, *values) -> DataFrame:
    def process_value(value, df_schema):
        try:
            col_ref = F.col(str(value))
            schema_field = next(
                field for field in df_schema.fields if field.name == str(value)
            )

            if isinstance(schema_field.dataType, ArrayType):
                return F.concat(F.lit("["), F.array_join(col_ref, ","), F.lit("]"))
            else:
                return F.col(str(value)).cast("string")
        except (StopIteration, AttributeError):
            return F.lit(str(value))

    col_expressions = [F.lit(str(kind))]
    col_expressions.extend(process_value(value, df.schema) for value in values)

    return df.withColumn(
        hash_col_name, F.sha2(F.concat_ws("||", *col_expressions), 256)
    )


def replace_10e30_with_null(df: DataFrame, col_name: str, new_col: str) -> DataFrame:
    # Petra tends to use 1e+30 for doubles.
    def replace_value(col_expr):
        return F.when(col_expr == F.lit(1e30), F.lit(None)).otherwise(col_expr)

    schema_field = next(field for field in df.schema.fields if field.name == col_name)

    if isinstance(schema_field.dataType, ArrayType):
        if isinstance(schema_field.dataType.elementType, ArrayType):
            transformation = F.transform(
                F.col(col_name),
                lambda outer_array: F.transform(outer_array, replace_value),
            )
        else:
            transformation = F.transform(F.col(col_name), replace_value)
    else:
        transformation = replace_value(F.col(col_name))

    return df.withColumn(new_col, transformation)


def int_to_boolean(df: DataFrame, col_name: str, new_col: str) -> DataFrame:
    def to_boolean(col_expr):
        return F.when(col_expr == F.lit(1), F.lit(True)).otherwise(F.lit(False))

    schema_field = next(field for field in df.schema.fields if field.name == col_name)

    if isinstance(schema_field.dataType, ArrayType):
        transformation = F.transform(F.col(col_name), to_boolean)
    else:
        transformation = to_boolean(F.col(col_name))

    return df.withColumn(new_col, transformation)


def upsert_dataframe_to_table(
    source_df, catalog_table, merge_key="id", add_metadata=True
):
    """
    Upsert a DataFrame to a Unity Catalog Delta table with automatic table creation.
    Works in both local Databricks Connect and Databricks workspace environments.
    """
    from delta.tables import DeltaTable
    from pyspark.sql.functions import current_timestamp, lit
    import uuid

    # Should work in Databricks or locally in vscode
    try:
        # Try to use existing spark session (Databricks workspace or VS Code extension)
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()

        if spark is None:
            # If no active session, create one (for Databricks Connect)
            from databricks.connect import DatabricksSession

            spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        # Fallback for environments where databricks.connect is not available
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if add_metadata:
        batch_id = str(uuid.uuid4())[:8]
        df_processed = source_df.withColumn(
            "last_updated", current_timestamp()
        ).withColumn("processing_batch", lit(batch_id))
    else:
        df_processed = source_df

    df_clean = df_processed.dropDuplicates([merge_key])

    if spark.catalog.tableExists(catalog_table):
        delta_table = DeltaTable.forName(spark, catalog_table)
        delta_table.alias("target").merge(
            df_clean.alias("source"), f"target.{merge_key} = source.{merge_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print(f"Upserted data to existing table {catalog_table}")
        return "updated"
    else:
        df_clean.write.format("delta").mode("overwrite").saveAsTable(catalog_table)
        print(f"Created new table {catalog_table}")
        return "created"
