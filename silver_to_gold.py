
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    current_timestamp
)

# Silver to Gold

BASE_PATH = os.getenv(
    "FP_DATA_DIR",
    r"D:\Projects\Data Engineering\FP_Streaming Pipeline"
)

SILVER_DIR = os.path.join(BASE_PATH, "silver")
GOLD_DIR = os.path.join(BASE_PATH, "gold")


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SvitlanaSilverToGold")
        .getOrCreate()
    )

    try:
        # Читаємо таблиці Silver
        bio_df = spark.read.parquet(
            os.path.join(SILVER_DIR, "athlete_bio")
        )

        results_df = spark.read.parquet(
            os.path.join(
                SILVER_DIR,
                "athlete_event_results"
            )
        )

        print("Біологічні дані:")
        bio_df.show(5, truncate=False)

        print("Результати змагань:")
        results_df.show(5, truncate=False)

        # Перетворюємо зріст і вагу на числа
        bio_df = (
            bio_df
            .withColumn(
                "height",
                col("height").try_cast("double")
            )
            .withColumn(
                "weight",
                col("weight").try_cast("double")
            )
            .filter(
                col("height").isNotNull()
                & col("weight").isNotNull()
                & (~col("height").isNaN())
                & (~col("weight").isNaN())
            )
        )

        ''' 
         # Обьєднуємо таблиці за athlete_id
        joined_df = bio_df.join(
            results_df,
            on="athlete_id",
            how="inner"
        )
        '''

        # Перейменовуємо country_noc у таблиці спортсменів,
        # щоб уникнути однакових назв стовпців після об'єднання.
        bio_df = bio_df.withColumnRenamed(
            "country_noc",
            "bio_country_noc"
        )

        # Об'єднуємо інформацію про спортсменів
        # з результатами їхніх змагань за athlete_id.
        joined_df = bio_df.join(
            results_df,
            on="athlete_id",
            how="inner"
        )

        print("Об'єднані дані:")
        joined_df.show(10, truncate=False)

        # Обчислюємо середні значення для sport, medal, sex, country_noc
        
        gold_df = (
            joined_df
            .groupBy(
                "sport",
                "medal",
                "sex",
                "country_noc"
            )
            .agg(
                avg("weight").alias("avg_weight"),
                avg("height").alias("avg_height")
            )
            .withColumn(
                "timestamp",
                current_timestamp()
            )
        )

        # Виводимо фінальні результати
        print("Фінальна таблиця Gold:")
        gold_df.show(20, truncate=False)

        # Записуємо результати у Gold
        output_path = os.path.join(
            GOLD_DIR,
            "avg_stats"
        )

        (
            gold_df.write
            .mode("overwrite")
            .parquet(output_path)
        )

        print(f"Gold створено: {output_path}")
        print("Silver to Gold завершено успішно.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
