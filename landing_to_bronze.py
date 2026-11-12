
import os
import requests
from pyspark.sql import SparkSession

# Landing to Bronze

# Основна папка проєкту
BASE_PATH = os.getenv(
    "FP_DATA_DIR",
    r"D:\Projects\Data Engineering\FP_Streaming Pipeline"
)

SOURCE_URL = "https://ftp.goit.study/neoversity/"

TABLES = [
    "athlete_bio",
    "athlete_event_results"
]

LANDING_DIR = os.path.join(BASE_PATH, "landing")
BRONZE_DIR = os.path.join(BASE_PATH, "bronze")


def main():
    os.makedirs(LANDING_DIR, exist_ok=True)
    os.makedirs(BRONZE_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SvitlanaLandingToBronze")
        .getOrCreate()
    )

    try:
        for table in TABLES:
            # Завантаження CSV з FTP-сервера
            filename = f"{table}.csv"
            url = SOURCE_URL + filename

            local_path = os.path.join(
                LANDING_DIR, filename
            )

            print(f"Завантажуємо: {url}")

            response = requests.get(
                url,
                timeout=120
            )
            response.raise_for_status()

            with open(local_path, "wb") as file:
                file.write(response.content)

            print(f"CSV збережено: {local_path}")

            # Зчитування CSV через Spark
            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(local_path)
            )

            print(f"Дані таблиці {table}:")
            df.show(10, truncate=False)

            # Запис у Bronze у форматі Parquet
            output_path = os.path.join(
                BRONZE_DIR, table
            )

            (
                df.write
                .mode("overwrite")
                .parquet(output_path)
            )

            print(f"Bronze створено: {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
