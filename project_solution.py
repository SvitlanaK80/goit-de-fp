
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator
)

# Автоматизація через Airflow

# Шлях усередині Linux-контейнера Airflow
PROJECT_DIR = os.getenv(
    "FP_PROJECT_DIR",
    "/opt/airflow/dags/final_project"
)

# Шлях до даних усередині контейнера
DATA_DIR = os.getenv(
    "FP_DATA_DIR",
    "/opt/airflow/data/final_project"
)

# Загальні параметри DAG
default_args = {
    "owner": "svitlana",
    "start_date": datetime(2026, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Створюємо DAG
with DAG(
    dag_id="svitlana_olympic_batch_processing",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["data_engineering", "final_project"]
) as dag:

    # Landing to Bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=os.path.join(
            PROJECT_DIR,
            "landing_to_bronze.py"
        ),
        conn_id="spark_default",
        conf={
            "spark.executorEnv.FP_DATA_DIR": DATA_DIR
        },
        env_vars={
            "FP_DATA_DIR": DATA_DIR
        },
        verbose=True
    )

    # Bronze to Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=os.path.join(
            PROJECT_DIR,
            "bronze_to_silver.py"
        ),
        conn_id="spark_default",
        conf={
            "spark.executorEnv.FP_DATA_DIR": DATA_DIR
        },
        env_vars={
            "FP_DATA_DIR": DATA_DIR
        },
        verbose=True
    )

    # Silver to Gold
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=os.path.join(
            PROJECT_DIR,
            "silver_to_gold.py"
        ),
        conn_id="spark_default",
        conf={
            "spark.executorEnv.FP_DATA_DIR": DATA_DIR
        },
        env_vars={
            "FP_DATA_DIR": DATA_DIR
        },
        verbose=True
    )

    # Послідовність виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
