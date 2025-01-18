from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

LOGIN = 'test'
BASE_DATA_PATH = 'my_path'
LINEITEMS_BLOCK = 'lineitems'
ORDERS_BLOCK = 'orders'
CUSTOMERS_BLOCK = 'customers'
SUPPLIERS_BLOCK = 'suppliers'
PARTS_BLOCK = 'parts'
SUCCESS_STATUS = 'Job successfully completed!'
NATION_PATH = f'{BASE_DATA_PATH}/nation'
REGION_PATH = f'{BASE_DATA_PATH}/region'
CUSTOMER_PATH = f'{BASE_DATA_PATH}/customer'
SUPPLIER_PATH = f'{BASE_DATA_PATH}/supplier'
LINEITEM_PATH = f'{BASE_DATA_PATH}/lineitem'
ORDERS_PATH = f'{BASE_DATA_PATH}/orders'
PART_PATH = f'{BASE_DATA_PATH}/part'
PARTSUPP_PATH = f'{BASE_DATA_PATH}/partsupp'
REPORT_TAG = '_report'

# поля для join и group ny
c_cust_key = 'C_CUSTKEY'
c_nationkey = 'C_NATIONKEY'
c_acc_bal = 'C_ACCTBAL'
c_mseg = 'C_MKTSEGMENT'
n_nationkey = 'N_NATIONKEY'
n_name = 'N_NAME'
n_regionkey = 'N_REGIONKEY'
s_supkey = 'S_SUPPKEY'
s_natkey = 'S_NATIONKEY'
s_act_bal = 'S_ACCTBAL'
r_regionkey = 'R_REGIONKEY'
r_name = 'R_NAME'
l_extprice = 'L_EXTENDEDPRICE'
l_dics = 'L_DISCOUNT'
l_tax = 'L_TAX'
l_sp_date = 'L_SHIPDATE'
l_rp_date = 'L_RECEIPTDATE'
l_ret_flg = 'L_RETURNFLAG'
l_orderkey = 'L_ORDERKEY'
o_cust_key = 'O_CUSTKEY'
o_ord_date = 'O_ORDERDATE'
o_ord_key = 'O_ORDERKEY'
o_total_price = 'O_TOTALPRICE'
o_ord_status = 'O_ORDERSTATUS'
o_ord_priority = 'O_ORDERPRIORITY'
ps_partkey = 'PS_PARTKEY'
p_partkey = 'P_PARTKEY'
ps_supkey = 'PS_SUPPKEY'
ps_sup_cost = 'PS_SUPPLYCOST'
p_ret_price = 'P_RETAILPRICE'
p_size = 'P_SIZE'

# отчет по customers
unique_customers_count = 'unique_customers_count'
avg_acctbal = 'avg_acctbal'
mean_acctbal = 'mean_acctbal'
min_acctbal = 'min_acctbal'
max_acctbal = 'max_acctbal'

l_cust_col_ord = [r_name, n_name, c_mseg, unique_customers_count,
                 avg_acctbal, mean_acctbal, min_acctbal, max_acctbal]

# отчет по lineitems
c_count = 'count'
sum_extendprice = 'sum_extendprice'
mean_discount = 'mean_discount'
mean_tax = 'mean_tax'
delivery_days = 'delivery_days'
a_return_flags = 'A_return_flags'
r_return_flags = 'R_return_flags'
n_return_flags = 'N_return_flags'

l_lineitems_col_ord = [l_orderkey, c_count, sum_extendprice, mean_discount,
                       mean_tax, delivery_days, a_return_flags, r_return_flags,
                       n_return_flags]

# отчет по orders
o_month = 'O_MONTH'
orders_count = 'orders_count'
avg_order_price = 'avg_order_price'
sum_order_price = 'sum_order_price'
min_order_price = 'min_order_price'
max_order_price = 'max_order_price'
f_order_status = 'f_order_status'
o_order_status = 'o_order_status'
p_order_status = 'p_order_status'

l_orders_col_ord = [o_month, n_name, o_ord_priority, orders_count, avg_order_price,
                       sum_order_price, min_order_price, max_order_price,
                       f_order_status, o_order_status, p_order_status]

# отчет по parts
p_type = 'P_TYPE'
p_container = 'P_CONTAINER'
parts_count = 'parts_count'
avg_retailprice = 'avg_retailprice'
size = 'size'
mean_retailprice = 'mean_retailprice'
min_retailprice = 'min_retailprice'
max_retailprice = 'max_retailprice'
avg_supplycost = 'avg_supplycost'
mean_supplycost = 'mean_supplycost'
min_supplycost = 'min_supplycost'
max_supplycost = 'max_supplycost'

l_parts_col_ord = [n_name, p_type, p_container, parts_count, avg_retailprice,
                       size, mean_retailprice, min_retailprice, max_retailprice,
                       avg_supplycost, mean_supplycost, min_supplycost, max_supplycost]

# отчет по suppliers
unique_supplers_count = 'unique_supplers_count'
avg_acctbal = 'avg_acctbal'
mean_acctbal = 'mean_acctbal'
min_acctbal = 'min_acctbal'
max_acctbal = 'max_acctbal'

l_supplier_col_ord = [r_name, n_name, unique_supplers_count, avg_acctbal,
                         mean_acctbal, min_acctbal, max_acctbal]

schema_customer = StructType([
        StructField(c_cust_key, IntegerType(), False),
        StructField('C_NAME', StringType(), True),
        StructField('C_ADDRESS', StringType(), True),
        StructField(c_nationkey, IntegerType(), False),
        StructField('C_PHONE', StringType(), True),
        StructField(c_acc_bal, DoubleType(), True),
        StructField(c_mseg, StringType(), True),
        StructField('C_COMMENT', StringType(), True),
    ])

schema_nation = StructType([
        StructField(n_nationkey, LongType(), False),
        StructField(n_name, StringType(), True),
        StructField(n_regionkey, LongType(), False),
        StructField('N_COMMENT', StringType(), True),
    ])

schema_region = StructType([
        StructField(r_regionkey, LongType(), False),
        StructField(r_name, StringType(), True),
        StructField('R_COMMENT', StringType(), True),
    ])

schema_lineitem = StructType([
        StructField(l_orderkey, LongType(), False),
        StructField('L_PARTKEY', IntegerType(), False),
        StructField('L_SUPPKEY', IntegerType(), False),
        StructField('L_LINENUMBER', IntegerType(), False),
        StructField('L_QUANTITY', IntegerType(), True),
        StructField(l_extprice, DoubleType(), True),
        StructField(l_dics, DoubleType(), True),
        StructField(l_tax, DoubleType(), True),
        StructField(l_ret_flg, StringType(), True),
        StructField('L_LINESTATUS', StringType(), True),
        StructField(l_sp_date, StringType(), True),
        StructField('L_COMMITDATE', StringType(), True),
        StructField(l_rp_date, StringType(), True),
        StructField('L_SHIPINSTRUCT', StringType(), True),
        StructField('L_SHIPMODE', StringType(), True),
        StructField('L_COMMENT', StringType(), True),
    ])

schema_orders = StructType([
        StructField(o_ord_key, LongType(), False),
        StructField(o_cust_key, LongType(), False),
        StructField(o_ord_status, StringType(), True),
        StructField(o_total_price, DoubleType(), True),
        StructField(o_ord_date, StringType(), True),
        StructField(o_ord_priority, StringType(), True),
        StructField('O_CLERK', StringType(), True),
        StructField('O_SHIPPRIORITY', IntegerType(), True),
        StructField('O_COMMENT', StringType(), True),
    ])

schema_supplier = StructType([
        StructField(s_supkey, LongType(), False),
        StructField('S_NAME', StringType(), True),
        StructField('S_ADDRESS', StringType(), True),
        StructField(s_natkey, LongType(), True),
        StructField('S_PHONE', StringType(), True),
        StructField(s_act_bal, DoubleType(), True),
        StructField('S_COMMENT', StringType(), True),
    ])

schema_parts = StructType([
        StructField(p_partkey, LongType(), False),
        StructField('P_NAME', StringType(), True),
        StructField('P_MFGR', StringType(), True),
        StructField('P_BRAND', StringType(), True),
        StructField(p_type, StringType(), True),
        StructField(p_size, LongType(), True),
        StructField(p_container, StringType(), True),
        StructField(p_ret_price, DoubleType(), True),
        StructField('P_COMMENT', StringType(), True),
    ])

schema_partsupp = StructType([
    StructField(ps_partkey, LongType(), False),
    StructField(ps_supkey, LongType(), False),
    StructField('PS_AVAILQTY', IntegerType(), True),
    StructField(ps_sup_cost, DoubleType(), True),
    StructField('PS_COMMENT', StringType(), True),
])

# sql-типы для GP
l_lineitems_types = ['BIGINT', 'BIGINT', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'BIGINT', 'BIGINT', 'BIGINT']
l_orders_type = ['TEXT', 'TEXT', 'TEXT', 'BIGINT', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'BIGINT', 'BIGINT', 'BIGINT']
l_cust_types = ['TEXT', 'TEXT', 'TEXT', 'BIGINT', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'FLOAT8']
l_suppl_types = ['TEXT', 'TEXT', 'BIGINT', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'FLOAT8']
l_part_types = ['TEXT', 'TEXT', 'TEXT', 'BIGINT', 'FLOAT8', 'BIGINT', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'FLOAT8', 'FLOAT8']
