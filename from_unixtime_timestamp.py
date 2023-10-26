from pyspark.sql.functions import from_unixtime
from pyspark.sql import DataFrame

def convert_unix_columns_to_timestamp(df: DataFrame, unix_columns: list):
    for unix_column, output_column in unix_columns:
        df = df.withColumn(output_column, from_unixtime(unix_column))
    return df
