
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Bronze to Silver

BASE_PATH = os.getenv(
    "FP_DATA_DIR",
    r"D:\Projects\Data Engineering\FP_Streaming Pipeline"
)

BRONZE_DIR = os.path.join(BASE_PATH, "bronze")
SILVER_DIR = os.path.join(BASE_PATH, "silver")

TABLES = [
    "athlete_bio",
    "athlete_event_results"
]


#Функція очищення тексту
def clean_text(text):
    if text is None:
        return None

    # Прибираємо зайві спеціальні символи
    return re.sub(
        r"""[^a-zA-Z0-9,."' ]""",
        "",
        str(text)
    ).strip()


#Створюємо Spark UDF
clean_text_udf = udf(
    clean_text,
    StringType()
)


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SvitlanaBronzeToSilver")
        .getOrCreate()
    )

    try:
        for table in TABLES:
            # Читаємо Bronze
            input_path = os.path.join(
                BRONZE_DIR, table
            )

            df = spark.read.parquet(input_path)

            # Очищуємо всі текстові колонки
            for column_name, dtype in df.dtypes:
                if dtype == "string":
                    df = df.withColumn(
                        column_name,
                        clean_text_udf(col(column_name))
                    )

            # Видаляємо дублікати
            df = df.dropDuplicates()

            print(f"Очищені дані: {table}")
            df.show(10, truncate=False)

            # Зберігаємо Silver
            output_path = os.path.join(
                SILVER_DIR, table
            )

            (
                df.write
                .mode("overwrite")
                .parquet(output_path)
            )

            print(f"Silver створено: {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
