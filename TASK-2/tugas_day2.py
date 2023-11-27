from airflow import DAG
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
import json
import pandas as pd


with DAG(
    dag_id = "tugas_day2",
    schedule=None,
    start_date=datetime(2023, 11, 16), 
    catchup = False
) as dag:
    
    def load_to_postgres(**kwargs):
        response_data = kwargs['ti'].xcom_pull(task_ids='predict_multiple_name')
        table = """
        CREATE TABLE IF NOT EXISTS gender_name_prediction (
            input JSON,
            details JSON,
            result_found BOOLEAN,
            first_name VARCHAR(255),
            probability FLOAT,
            gender VARCHAR(50),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
        pg_hook = PostgresHook(postgres_conn_id='pg_conn_id')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(table)
        conn.commit()
        result = json.loads(response_data)

        cursor.execute("""
            INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            json.dumps(result.get("input")),
            json.dumps(result.get("details")),
            result.get("result_found"),
            result.get("first_name"),
            result.get("probability"),
            result.get("gender"),
        ))
        
        conn.commit()
        conn.close()

    identify_name = SimpleHttpOperator(
        task_id="predict_multiple_name",
        endpoint="/gender",
        method="POST",
        data={"first_name":"Sandra"},
        http_conn_id="gen_api",
        log_response=True,
    )

    load_data_to_task = PythonOperator(
        task_id= 'ingest',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    identify_name >> load_data_to_task