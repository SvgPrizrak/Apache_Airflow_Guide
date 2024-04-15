from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    dag_id='test_postgres_dag',
    start_date=datetime(2024, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_local_test",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id INT NOT NULL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            owner VARCHAR NOT NULL);
          """
    )

    insert_to_pet_table = PostgresOperator(
        task_id="insert_to_pet_table",
        postgres_conn_id="postgres_local_test",
        sql="""
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (1, 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (2, 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (3, 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (pet_id, name, pet_type, birth_date, owner)
            VALUES (4, 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """
    )

    delete_from_pet_table = PostgresOperator(
        task_id="delete_from_pet_table",
        postgres_conn_id="postgres_local_test",
        sql="""
            DELETE FROM pet WHERE pet_id <= 4
            """
    )

    create_pet_table >> delete_from_pet_table >> insert_to_pet_table