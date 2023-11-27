from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

with DAG(
    'tugas_day_satu',
    description='nugas',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2023, 11, 20),
    catchup=False,
) as dag:
    def push_var_to_xcom(ti=None):
        ti.xcom_push(key='book_title', value='Aku Ingin Pintar')
        ti.xcom_push(key='author', value='siGanteng')

    def pull_multiple(**kwargs):
        ti = kwargs['ti']
        buku = ti.xcom_pull(task_ids='tugas_day_one', key='book_title')
        penulis = ti.xcom_pull(task_ids='tugas_day_one', key='author')

        print(f'Judulnya: {buku}')
        print(f'Penulisnya: {penulis}')

    push_var = PythonOperator(
        task_id = 'tugas_day_one',
        python_callable = push_var_to_xcom,
        provide_context=True
    )
    pull_multiple = PythonOperator(
        task_id='hasil_pull',
        python_callable=pull_multiple,
        provide_context=True
    )
    
    push_var >> pull_multiple