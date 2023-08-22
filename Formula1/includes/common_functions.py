# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

from pyspark.sql import DataFrame

def set_partition_column(df: DataFrame, partition_col: str) -> DataFrame:
    column_names = df.columns

    if partition_col in column_names:
        column_names.remove(partition_col)
        column_names.append(partition_col)
        return df.select(*column_names)
    else:
        raise ValueError(f"'{partition_col}' is not a column in the DataFrame.")


# COMMAND ----------

from pyspark.sql import DataFrame

def df_inc_write_to_table(database: str, table: str, df: DataFrame, partition_col: str) -> None:
    try:
        df = set_partition_column(df,partition_col)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        if (spark._jsparkSession.catalog().tableExists(f"{database}.{table}")):
            df.write.mode("overwrite").insertInto(f"{database}.{table}")
        else:
            df.write.mode("overwrite").partitionBy(partition_col).format("parquet").saveAsTable(f"{database}.{table}")
        print("write successful")
    except Exception as e:
        print(f"write NOT successful: {str(e)}")

