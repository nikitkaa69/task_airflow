from airflow.decorators import dag, task
import pendulum
import os
import re
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
from pandas.errors import EmptyDataError
from airflow.datasets import Dataset

FILE_TO_PROCESS = "tiktok_google_play_reviews.csv"
FILE_PATH_IN_DOCKER = f"/opt/airflow/data/{FILE_TO_PROCESS}"
FILE_ENCODING = 'latin-1'
INTERIM_FILE_PATH = "/opt/airflow/data/interim_data.json"
PROCESSED_DATA_DATASET = Dataset("file://project/processed_data.json")


@dag(
    dag_id="project_dag_1_processing",
    start_date=pendulum.today('UTC'),
    schedule=None,
    catchup=False,
)
def data_processing_workflow():
    @task.sensor(task_id="wait_for_file", poke_interval=10, timeout=300, mode="poke")
    def wait_for_file_sensor():
        if os.path.exists(FILE_PATH_IN_DOCKER):
            return True
        else:
            return False

    @task.branch
    def check_if_file_is_empty():
        try:
            df_test = pd.read_csv(FILE_PATH_IN_DOCKER, nrows=1, encoding=FILE_ENCODING)
            if df_test.empty:
                return "log_empty_file_task"
            else:
                return "processing_task_group"
        except EmptyDataError:
            return "log_empty_file_task"
        except Exception as e:
            print(f"Ошибка чтения файла: {e}")
            return "log_empty_file_task"

    log_empty_file_task = BashOperator(
        task_id="log_empty_file_task",
        bash_command=f"echo 'Файл {FILE_TO_PROCESS} пуст или поврежден'"
    )

    with TaskGroup("processing_task_group") as processing_group:

        @task(execution_timeout=pendulum.duration(minutes=20))
        def load_and_save_to_interim():
            print(f"Загружаю {FILE_PATH_IN_DOCKER}")
            df = pd.read_csv(FILE_PATH_IN_DOCKER, encoding=FILE_ENCODING)
            print(f"Успешно загружено {len(df)} строк")

            print(f"Сохраняю в промежуточный файл {INTERIM_FILE_PATH}")
            df.to_json(INTERIM_FILE_PATH, orient="records")

            return INTERIM_FILE_PATH

        @task(execution_timeout=pendulum.duration(minutes=20))
        def replace_nulls(interim_file_path: str):
            print(f"Читаю файл для замены 'null' {interim_file_path}")
            df = pd.read_json(interim_file_path, orient="records")

            print("Заменяю 'null' значения на '-'")
            df.fillna('-', inplace=True)

            print(f"Перезаписываю файл {interim_file_path}")
            df.to_json(interim_file_path, orient="records")

            return interim_file_path

        @task(execution_timeout=pendulum.duration(minutes=20))
        def sort_data_by_date(interim_file_path: str):
            DATE_COLUMN_NAME = 'at'

            print(f"Читаю файл для сортировки {interim_file_path}")
            df = pd.read_json(interim_file_path, orient="records")

            print(f"Конвертирую '{DATE_COLUMN_NAME}' в datetime")

            DATE_FORMAT = "%d/%m/%Y %H:%M"
            df[DATE_COLUMN_NAME] = pd.to_datetime(
                df[DATE_COLUMN_NAME],
                format=DATE_FORMAT,
                errors='coerce'
            )

            print(f"Сортирую по '{DATE_COLUMN_NAME}'")
            df.sort_values(by=DATE_COLUMN_NAME, inplace=True)

            print(f"Перезаписываю отсортированный файл {interim_file_path}")
            df.to_json(interim_file_path, orient="records", date_format='iso')

            return interim_file_path

        @task(execution_timeout=pendulum.duration(minutes=20), outlets=[PROCESSED_DATA_DATASET])
        def clean_content_column(interim_file_path: str):
            print(f"Читаю файл {interim_file_path} для очистки 'content'")
            df = pd.read_json(interim_file_path, orient="records")

            def clean_text(text):
                text_str = str(text)
                cleaned_text = re.sub(r'[^\x00-\x7F]+', ' ', text_str)
                return cleaned_text

            print("Очищаю колонку 'content' от не-ASCII символов")
            df['content'] = df['content'].apply(clean_text)

            print(f"Перезаписываю финальный файл {interim_file_path}")
            df.to_json(interim_file_path, orient="records", date_format='iso')

            return interim_file_path

        interim_path_xcom = load_and_save_to_interim()
        nulls_replaced_xcom = replace_nulls(interim_path_xcom)
        sorted_xcom = sort_data_by_date(nulls_replaced_xcom)
        clean_xcom = clean_content_column(sorted_xcom)

    wait_for_file_sensor() >> check_if_file_is_empty() >> [log_empty_file_task, processing_group]


data_processing_workflow()
