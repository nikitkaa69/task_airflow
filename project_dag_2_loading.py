from airflow.decorators import dag, task
from airflow.datasets import Dataset
import pendulum
import pandas as pd
from airflow.providers.mongo.hooks.mongo import MongoHook

PROCESSED_DATA_DATASET = Dataset("file://project/processed_data.json")
INTERIM_FILE_PATH = "/opt/airflow/data/interim_data.json"

MONGO_CONNECTION_ID = "mongo_default"
MONGO_DATABASE = "tiktok_project_db"
MONGO_COLLECTION = "reviews"


@dag(
    dag_id="project_dag_2_loading",
    start_date=pendulum.today('UTC'),
    schedule=[PROCESSED_DATA_DATASET],
    catchup=False
)
def data_loading_workflow():
    @task(execution_timeout=pendulum.duration(minutes=20))
    def load_processed_data_to_mongo():
        print(f"DAG_2 'проснулся' по сигналу от DAG_1")
        print(f"Читаю обработанный файл {INTERIM_FILE_PATH}")
        df = pd.read_json(INTERIM_FILE_PATH, orient="records")
        print(f"Прочитано {len(df)} строк из JSON")

        print(f"Подключаюсь к Mongo (Conn ID: {MONGO_CONNECTION_ID})")
        hook = MongoHook(conn_id=MONGO_CONNECTION_ID)
        client = hook.get_conn()
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        print(f"Успешно подключился к БД: {MONGO_DATABASE}, коллекция: {MONGO_COLLECTION}")

        print("Очищаю старые данные")
        collection.delete_many({})

        print("Конвертирую DataFrame в список словарей")
        records = df.to_dict('records')

        print(f"Загружаю {len(records)} записей в MongoDB")
        collection.insert_many(records)
        print("Загрузка в MongoDB завершена")

    load_processed_data_to_mongo()


data_loading_workflow()
