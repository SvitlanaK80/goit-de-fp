
import os
from getpass import getpass

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, avg, current_timestamp,
    to_json, struct, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# Налаштування 
MYSQL_HOST = "217.61.57.46"
MYSQL_DATABASE = "olympic_dataset"
MYSQL_USER = "neo_data_admin"

KAFKA_SERVER = "77.81.230.104:9092"
KAFKA_USER = "admin"

INPUT_TOPIC = "athlete_event_results"
OUTPUT_TOPIC = "svitlana_athlete_enriched_agg"
OUTPUT_TABLE = "svitlana_athlete_avg_stats"

MYSQL_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/"
    f"{MYSQL_DATABASE}"
)


def main():
    mysql_password = os.getenv("MYSQL_PASSWORD") or getpass(
        "MySQL password: "
    )

    kafka_password = os.getenv("KAFKA_PASSWORD") or getpass(
        "Kafka password: "
    )

    # Створюємо SparkSession
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SvitlanaOlympicStreaming")
        .config(
            "spark.jars.packages",
            # "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0,"
            "com.mysql:mysql-connector-j:9.2.0"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Зчитуємо фізичні дані атлетів із MySQL
    bio_df = (
        spark.read
        .format("jdbc")
        .option("url", MYSQL_URL)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_bio")
        .option("user", MYSQL_USER)
        .option("password", mysql_password)
        .load()
    )

    # Перетворюємо зріст і вагу на числа
    bio_df = (
        bio_df
        .withColumn("height", col("height").try_cast("double"))
        .withColumn("weight", col("weight").try_cast("double"))
        .filter(
            col("height").isNotNull()
            & col("weight").isNotNull()
            & (~col("height").isNaN())
            & (~col("weight").isNaN())
        )
        .select(
            "athlete_id",
            "sex",
            "height",
            "weight"
        )
        .dropDuplicates(["athlete_id"])
    )

    print("Етапи 1-2. Очищені біологічні дані:")
    bio_df.show(10, truncate=False)

    # Зчитуємо результати з Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.'
            'PlainLoginModule required '
            f'username="{KAFKA_USER}" '
            f'password="{kafka_password}";'
        )
        .load()
    )

    # Перетворюємо JSON на DataFrame
    schema = StructType([
        StructField("athlete_id", LongType()),
        StructField("sport", StringType()),
        StructField("medal", StringType()),
        StructField("country_noc", StringType())
    ])

    results_df = (
        kafka_df
        .select(
            from_json(
                col("value").cast("string"),
                schema
            ).alias("data")
        )
        .select("data.*")
        .filter(col("athlete_id").isNotNull())
    )

    # Обьєднуємо Kafka та MySQL за athlete_id
    joined_df = results_df.join(
        bio_df,
        on="athlete_id",
        how="inner"
    )

    def process_batch(batch_df, batch_id):
        # Обробляємо один мікробатч
        if batch_df.isEmpty():
            return

        # Розраховуємо середній зріст і вагу
        avg_df = (
            batch_df
            .groupBy(
                "sport",
                "medal",
                "sex",
                "country_noc"
            )
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight")
            )
            .withColumn(
                "timestamp",
                current_timestamp()
            )
        )

        avg_df.persist()

        try:
            print(f"Мікробатч: {batch_id}")
            avg_df.show(20, truncate=False)

            # Запис у вихідний Kafka-топік
            kafka_output = avg_df.select(
                to_json(
                    struct(*avg_df.columns)
                ).alias("value")
            )

            (
                kafka_output.write
                .format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    KAFKA_SERVER
                )
                .option("topic", OUTPUT_TOPIC)
                .option(
                    "kafka.security.protocol",
                    "SASL_PLAINTEXT"
                )
                .option(
                    "kafka.sasl.mechanism",
                    "PLAIN"
                )
                .option(
                    "kafka.sasl.jaas.config",
                    'org.apache.kafka.common.security.plain.'
                    'PlainLoginModule required '
                    f'username="{KAFKA_USER}" '
                    f'password="{kafka_password}";'
                )
                .save()
            )

            # Запис результатів у MySQL
            (
                avg_df.write
                .format("jdbc")
                .option("url", MYSQL_URL)
                .option(
                    "driver",
                    "com.mysql.cj.jdbc.Driver"
                )
                .option("dbtable", OUTPUT_TABLE)
                .option("user", MYSQL_USER)
                .option("password", mysql_password)
                .mode("append")
                .save()
            )

            print(
                f"Мікробатч {batch_id} записано "
                "до Kafka та MySQL"
            )

        finally:
            avg_df.unpersist()

    # Запускаємо Structured Streaming
    query = (
        joined_df.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "checkpoints",
                "svitlana_olympic_stream"
            )
        )
        .start()
    )

    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
