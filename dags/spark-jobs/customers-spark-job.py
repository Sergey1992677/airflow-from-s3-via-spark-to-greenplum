from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from dags.spark_utils import *
from dags.common_vars import *

TARGET_PATH = f's3a://{LOGIN}/{CUSTOMERS_BLOCK}{REPORT_TAG}'

def main():

    appl_nm = make_spark_appl_nm(CUSTOMERS_BLOCK, LOGIN)

    spark = make_spark_session(appl_nm)

    df_customer = read_parquet(spark, schema_customer, CUSTOMER_PATH)

    df_nation = read_parquet(spark, schema_nation, NATION_PATH)

    df_region = read_parquet(spark, schema_region, REGION_PATH)

    cond_cust_nation = F.col(c_nationkey) == F.col(n_nationkey)

    cond_nation_region = F.col(n_regionkey) == F.col(r_regionkey)

    df_result = df_customer \
                    .join(broadcast(df_nation), cond_cust_nation, how='left') \
                    .join(broadcast(df_region), cond_nation_region, how='left') \
                    .select([r_name, n_name, c_mseg, c_cust_key, c_acc_bal]) \
                    .groupBy([r_name, n_name, c_mseg]) \
                    .agg(
                        F.countDistinct(c_cust_key).alias(unique_customers_count),
                        F.avg(c_acc_bal).alias(avg_acctbal),
                        F.mean(c_acc_bal).alias(mean_acctbal),
                        F.min(c_acc_bal).alias(min_acctbal),
                        F.max(c_acc_bal).alias(max_acctbal)
                        ) \
                    .select(l_cust_col_ord) \
                    .orderBy([n_name, c_mseg], ascending=[True, True])

    overwrite_parquet(df_result, TARGET_PATH)

    spark.stop()

    # лог
    print(SUCCESS_STATUS)


if __name__ == "__main__":
    main()
