from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['savichdmitrii@mail.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='test_clickhouse_dag',
    start_date=datetime(2024, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table = ClickHouseOperator(
        task_id="create_pet_table",
        database='default',
        clickhouse_conn_id="clickhouse_local_test",
        sql="""
            CREATE TABLE IF NOT EXISTS employee
            (emp_no  UInt32 NOT NULL)
            ENGINE = MergeTree()
            PRIMARY KEY (emp_no);
          """
    )

    create_pet_table