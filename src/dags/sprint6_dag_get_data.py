from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models.variable import Variable

from typing import Dict, List, Optional

import boto3
import contextlib
import pandas as pd
import pendulum
import vertica_python


AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )


def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host=Variable.get("VERTICA_HOST"),
        port=5433,
        user=Variable.get("VERTICA_USER"),
        password=Variable.get("VERTICA_PASSWORD")
    )
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()


# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла
bash_command_tmpl = """
echo {{ params.files }}
"""


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_data():
    load_list_info = [
        ('dialogs.csv', ['message_id', 'message_ts', 'message_from', 'message_to', 'message', 'message_group'], {'message_group': 'Int64'}),
        ('groups.csv', ['id', 'admin_id', 'group_name', 'registration_dt',
                        'is_private'], {}),
        ('users.csv', ['id', 'chat_name', 'registration_dt', 'country', 'age'], {'age': 'Int64'}),
        ('group_log.csv', ['group_id', 'user_id', 'user_id_from', 'group_event',
                           'event_timestamp'], {}),
    ]

    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{item_info[0]}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': item_info[0]},
        ) for item_info in load_list_info
    ]

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': " ".join(f'/data/{item_info[0]}' for item_info in load_list_info)}
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    load_tasks = [
            PythonOperator(
                task_id=f'load_{filename.split(".")[0]}',
                python_callable=load_dataset_file_to_vertica,
                op_kwargs={
                    'dataset_path': f'/data/{filename}',
                    'schema': 'stv2023091120__STAGING',
                    'table': filename.split('.')[0],
                    'columns': columns,
                    'type_override': type_override,
                }
            ) for filename, columns, type_override in load_list_info]

    fetch_tasks >> print_10_lines_of_each >> start >> load_tasks >> end


_ = sprint6_dag_get_data()