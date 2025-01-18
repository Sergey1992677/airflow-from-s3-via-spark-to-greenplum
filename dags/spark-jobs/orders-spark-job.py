from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from dags.spark_utils import *
from dags.common_vars import *

TARGET_PATH = f's3a://{LOGIN}/{ORDERS_BLOCK}{REPORT_TAG}'

def main():

    appl_nm = make_spark_appl_nm(ORDERS_BLOCK, LOGIN)

    spark = make_spark_session(appl_nm)

    l_ord_cols = [o_ord_key, o_cust_key, o_ord_date,
                  o_ord_priority, o_total_price, o_ord_status]

    df_order = read_parquet(spark, schema_orders, ORDERS_PATH) \
                .select(l_ord_cols) \
                .withColumn(o_month, F.substring(F.col(o_ord_date), 0, 7))

    df_customer = read_parquet(spark, schema_customer, CUSTOMER_PATH) \
                .select(c_cust_key, c_nationkey)

    df_nation = read_parquet(spark, schema_nation, NATION_PATH) \
                .select(n_nationkey, n_name)

    cond_cust_orders = F.col(o_cust_key) == F.col(c_cust_key)

    cond_cust_nation = F.col(c_nationkey) == F.col(n_nationkey)

    df_result = df_order \
                    .join(df_customer, cond_cust_orders, how='left') \
                    .join(broadcast(df_nation), cond_cust_nation, how='left') \
                    .groupBy([o_month, n_name, o_ord_priority]) \
                    .agg(F.count(o_ord_key).alias(orders_count),
                         F.avg(o_total_price).alias(avg_order_price),
                         F.sum(o_total_price).alias(sum_order_price),
                         F.min(o_total_price).alias(min_order_price),
                         F.max(o_total_price).alias(max_order_price),
                         F.sum(F.when(F.col(o_ord_status) == 'F', 1)
                               .otherwise(0)).alias(f_order_status),
                         F.sum(F.when(F.col(o_ord_status) == 'O', 1)
                               .otherwise(0)).alias(o_order_status),
                         F.sum(F.when(F.col(o_ord_status) == 'P', 1)
                               .otherwise(0)).alias(p_order_status)
                         ) \
                    .select(l_orders_col_ord) \
                    .orderBy([n_name, o_ord_priority], ascending=[True, True])

    overwrite_parquet(df_result, TARGET_PATH)

    spark.stop()

    # лог
    print(SUCCESS_STATUS)


if __name__ == "__main__":
    main()
