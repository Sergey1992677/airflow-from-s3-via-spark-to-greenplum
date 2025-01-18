from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from dags.spark_utils import *
from dags.common_vars import *

TARGET_PATH = f's3a://{LOGIN}/{PARTS_BLOCK}{REPORT_TAG}'

def main():

    appl_nm = make_spark_appl_nm(PARTS_BLOCK, LOGIN)

    spark = make_spark_session(appl_nm)

    df_parts = read_parquet(spark, schema_parts, PART_PATH) \
                .select(p_type, p_container, p_partkey, p_ret_price, p_size)

    df_partsupp = read_parquet(spark, schema_partsupp, PARTSUPP_PATH) \
                    .select(ps_partkey, ps_supkey, ps_sup_cost)

    df_supplier = read_parquet(spark, schema_supplier, SUPPLIER_PATH) \
                    .select(s_supkey, s_natkey)

    df_nation = read_parquet(spark, schema_nation, NATION_PATH) \
                .select(n_nationkey, n_name)

    cond_partsupp_part = F.col(ps_partkey) == F.col(p_partkey)

    cond_partsupp_supp = F.col(ps_supkey) == F.col(s_supkey)

    cond_sup_nation = F.col(s_natkey) == F.col(n_nationkey)

    df_result = df_partsupp \
        .join(df_parts, cond_partsupp_part, how='left') \
        .join(df_supplier, cond_partsupp_supp, how='left') \
        .join(broadcast(df_nation), cond_sup_nation, how='left') \
        .groupBy([n_name, p_type, p_container]) \
        .agg(F.countDistinct(p_partkey).alias(parts_count),
             F.avg(p_ret_price).alias(avg_retailprice),
             F.sum(p_size).alias(size),
             F.median(p_ret_price).alias(mean_retailprice),
             F.min(p_ret_price).alias(min_retailprice),
             F.max(p_ret_price).alias(max_retailprice),
             F.avg(ps_sup_cost).alias(avg_supplycost),
             F.median(ps_sup_cost).alias(mean_supplycost),
             F.min(ps_sup_cost).alias(min_supplycost),
             F.max(ps_sup_cost).alias(max_supplycost)
             ) \
        .select(l_parts_col_ord) \
        .orderBy([n_name, p_type, p_container], ascending=[True, True, True])

    overwrite_parquet(df_result, TARGET_PATH)

    spark.stop()

    # лог
    print(SUCCESS_STATUS)


if __name__ == "__main__":
    main()
