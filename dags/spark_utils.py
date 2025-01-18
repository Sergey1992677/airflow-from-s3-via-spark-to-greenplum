from pyspark.sql import SparkSession, DataFrame

def make_spark_session(appl_nm):
    return (SparkSession.builder
            .appName(appl_nm)
            .getOrCreate())


def make_spark_appl_nm(block, login):
    return f'spark-job-{login}-{block}-1'


def read_parquet(spark: SparkSession, file_schema, file_path) -> DataFrame:
    return spark \
        .read \
        .schema(file_schema) \
        .parquet(file_path)


def overwrite_parquet(df: DataFrame, out_path):
    df \
     .write \
     .mode("overwrite") \
     .parquet(out_path)