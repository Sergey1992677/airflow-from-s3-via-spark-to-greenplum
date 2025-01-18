"""
Даг работает без расписания
pipeline обработки данных, представленных из Теста TPC-H
"""

import pendulum as pm
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dags.common_vars import *
from dags.spark_utils import make_spark_appl_nm

def make_spark_oper_task_nm(block):
    return f'spark_{block}_submit'


def make_spark_sens_task_nm(block):
    return f'spark_{block}_monitor'


def make_gp_datamart_nm(block):
    return f'{block}_datamart'


def make_spark_appl_file_nm(block):
    return f'yamls/{block}_spark_submit.yaml'


def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_NS_SPARK,
        application_file=application_file,
        kubernetes_conn_id=KUBER_CONN_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, appl_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_NS_SPARK,
        application_name=appl_name,
        kubernetes_conn_id=KUBER_CONN_ID,
        attach_log=True,
        dag=link_dag
    )


def get_fields(block, common_dict):
    l_data = common_dict[block]
    l_fields = l_data[0]
    l_types = l_data[1]

    var_len_types = len(l_types)

    if var_len_types != len(l_fields):
        raise Exception('Число полей не соответствует числу типов полей!')

    l_fields_with_types = [f'{l_fields[i]} {l_types[i]}' for i in range(var_len_types)]

    return ','.join(l_fields_with_types)


def make_sql_to_create_tbl(block):
    return f"""DROP EXTERNAL TABLE IF EXISTS {LOGIN}.{block};
               CREATE EXTERNAL TABLE {LOGIN}.{block} ({get_fields(block, DATAMART_FLDS)})
               LOCATION ('pxf://{LOGIN}/{block}{REPORT_TAG}?PROFILE=s3:parquet&SERVER=default')
               ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';"""

# обшие переменные
K8S_NS_SPARK = 'project'
KUBER_CONN_ID = 'kubernetes'
GREENPLUM_ID = 'greenplume'
TASKS = [CUSTOMERS_BLOCK, LINEITEMS_BLOCK, ORDERS_BLOCK, PARTS_BLOCK, SUPPLIERS_BLOCK]
DATAMART_FLDS = {CUSTOMERS_BLOCK: [l_cust_col_ord, l_cust_types],
                 LINEITEMS_BLOCK: [l_lineitems_col_ord, l_lineitems_types],
                 ORDERS_BLOCK: [l_orders_col_ord, l_orders_type],
                 PARTS_BLOCK: [l_parts_col_ord, l_part_types],
                 SUPPLIERS_BLOCK: [l_supplier_col_ord, l_suppl_types], }

DEFAULT_ARGS = {
    'start_date': pm.datetime(2024, 3, 1, tz="UTC"),
    'owner': LOGIN
}

with DAG(
    dag_id=f'project-{LOGIN}-dag',
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=[LOGIN],
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    for val in TASKS:

        submit_task = _build_submit_operator(
            task_id=make_spark_oper_task_nm(val),
            application_file=make_spark_appl_file_nm(val),
            link_dag=dag
        )

        sensor_task = _build_sensor(
            task_id=make_spark_sens_task_nm(val),
            appl_name=make_spark_appl_nm(val, LOGIN),
            link_dag=dag
        )

        build_datamart = SQLExecuteQueryOperator(
            task_id=make_gp_datamart_nm(val),
            conn_id=GREENPLUM_ID,
            sql=make_sql_to_create_tbl(val),
            split_statements=True,
            autocommit=True,
            return_last=False,
        )

        start >> submit_task >> sensor_task >> build_datamart >> end
