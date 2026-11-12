
import os
import json
from getpass import getpass

import mysql.connector
from kafka import KafkaProducer

# Читаємо рез-ти змагань із MySQL
# та запис у Kafka-топік athlete_event_results

MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = 3306
MYSQL_USER = "neo_data_admin"
MYSQL_DATABASE = "olympic_dataset"

KAFKA_SERVER = "77.81.230.104:9092"
KAFKA_USER = "admin"
INPUT_TOPIC = "athlete_event_results"


def main():
    mysql_password = os.getenv("MYSQL_PASSWORD") or getpass(
        "MySQL password: "
    )

    kafka_password = os.getenv("KAFKA_PASSWORD") or getpass(
        "Kafka password: "
    )

    connection = None
    producer = None

    try:
        # Зчитуємо дані з MySQL
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=mysql_password,
            database=MYSQL_DATABASE,
            connection_timeout=20
        )

        cursor = connection.cursor(dictionary=True)

        cursor.execute("""
            SELECT
                athlete_id,
                sport,
                medal,
                country_noc
            FROM athlete_event_results
        """)

        # Створюємо Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=KAFKA_USER,
            sasl_plain_password=kafka_password,
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False, default=str
            ).encode("utf-8")
        )

        count = 0

        # Передаємо кожен запис у Kafka
        for row in cursor:
            producer.send(INPUT_TOPIC, value=row)
            count += 1

            if count <= 5:
                print(f"Відправлено: {row}")

            if count % 10000 == 0:
                print(f"Відправлено записів: {count}")

        producer.flush()

        print(f"Усього відправлено: {count}")
        print(f"Kafka-топік: {INPUT_TOPIC}")

    finally:
        if producer is not None:
            producer.close()

        if connection is not None and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    main()
