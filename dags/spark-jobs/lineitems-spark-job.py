from pyspark.sql import functions as F
from dags.spark_utils import *
from dags.common_vars import *

TARGET_PATH = f's3a://{LOGIN}/{LINEITEMS_BLOCK}{REPORT_TAG}'

def main():

    appl_nm = make_spark_appl_nm(LINEITEMS_BLOCK, LOGIN)

    spark = make_spark_session(appl_nm)

    df_result = read_parquet(spark, schema_lineitem, LINEITEM_PATH) \
                    .select(l_orderkey, l_extprice, l_dics, l_tax, l_rp_date, l_sp_date, l_ret_flg) \
                    .groupBy(l_orderkey) \
                    .agg(F.count(F.col(l_orderkey)).alias(c_count),
                         F.sum(F.col(l_extprice)).alias(sum_extendprice),
                         F.median(F.col(l_dics)).alias(mean_discount),
                         F.mean(F.col(l_tax)).alias(mean_tax),
                         F.mean(F.datediff(F.to_date(F.col(l_rp_date)),
                                           F.to_date(F.col(l_sp_date)))).alias(delivery_days),
                         F.sum(F.when(F.col(l_ret_flg) == 'A', 1)
                               .otherwise(0)).alias(a_return_flags),
                         F.sum(F.when(F.col(l_ret_flg) == 'R', 1)
                               .otherwise(0)).alias(r_return_flags),
                         F.sum(F.when(F.col(l_ret_flg) == 'N', 1)
                               .otherwise(0)).alias(n_return_flags)
                         ) \
                    .select(l_lineitems_col_ord) \
                    .orderBy(F.asc(l_orderkey))

    overwrite_parquet(df_result, TARGET_PATH)

    spark.stop()

    # лог
    print(SUCCESS_STATUS)


if __name__ == "__main__":
    main()
