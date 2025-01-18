from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from dags.spark_utils import *
from dags.common_vars import *

TARGET_PATH = f's3a://{LOGIN}/{SUPPLIERS_BLOCK}{REPORT_TAG}'

def main():

    appl_nm = make_spark_appl_nm(SUPPLIERS_BLOCK, LOGIN)

    spark = make_spark_session(appl_nm)

    df_supplier = read_parquet(spark, schema_supplier, SUPPLIER_PATH)

    df_nation = read_parquet(spark, schema_nation, NATION_PATH)

    df_region = read_parquet(spark, schema_region, REGION_PATH)

    cond_sup_nation = F.col(s_natkey) == F.col(n_nationkey)

    cond_nation_region = F.col(n_regionkey) == F.col(r_regionkey)

    df_result = df_supplier \
                    .join(broadcast(df_nation), cond_sup_nation, how='left') \
                    .join(broadcast(df_region), cond_nation_region, how='left') \
                    .groupBy([r_name, n_name]) \
                    .agg(F.countDistinct(s_supkey).alias(unique_supplers_count),
                        F.avg(s_act_bal).alias(avg_acctbal),
                        F.median(s_act_bal).alias(mean_acctbal),
                        F.min(s_act_bal).alias(min_acctbal),
                        F.max(s_act_bal).alias(max_acctbal)
                        ) \
                    .select(l_supplier_col_ord) \
                    .orderBy([n_name, r_name], ascending=[True, True])

    overwrite_parquet(df_result, TARGET_PATH)

    spark.stop()

    # лог
    print(SUCCESS_STATUS)


if __name__ == "__main__":
    main()
