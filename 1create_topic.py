
import os
from getpass import getpass
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Підготовка Kafka-топіку для результатів змагань
# Підготовка вихідного Kafka-топіку

KAFKA_SERVER = "77.81.230.104:9092"
KAFKA_USER = "admin"

INPUT_TOPIC = "athlete_event_results"
OUTPUT_TOPIC = "svitlana_athlete_enriched_agg"


def main():
    password = os.getenv("KAFKA_PASSWORD") or getpass(
        "Kafka password: "
    )

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_SERVER,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=password,
        request_timeout_ms=30000
    )

    try:
        existing_topics = set(admin.list_topics())

        for topic_name in [INPUT_TOPIC, OUTPUT_TOPIC]:
            if topic_name in existing_topics:
                print(f"Топік вже існує: {topic_name}")
                continue

            topic = NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )

            try:
                admin.create_topics([topic])
                print(f"Створено топік: {topic_name}")
            except TopicAlreadyExistsError:
                print(f"Топік вже існує: {topic_name}")

        print("Підготовка Kafka-топіків завершена.")

    finally:
        admin.close()


if __name__ == "__main__":
    main()
