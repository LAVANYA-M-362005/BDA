import json
import os
import time
from urllib.parse import quote

import requests
from hdfs import InsecureClient
from kafka import KafkaConsumer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales-transactions")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sales-hdfs-writer")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_INPUT_DIR = os.getenv("HDFS_INPUT_DIR", "/sales/input")


def connect_kafka():
    for attempt in range(30):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
        except KafkaError:
            if attempt == 29:
                raise
            time.sleep(2)
    raise RuntimeError("Kafka consumer connection attempts ended unexpectedly.")


def connect_hdfs():
    client = InsecureClient(HDFS_URL, user="root")
    for attempt in range(60):
        try:
            client.makedirs(HDFS_INPUT_DIR)
            return client
        except requests.exceptions.RequestException:
            if attempt == 59:
                raise
            time.sleep(2)
    raise RuntimeError("HDFS connection attempts ended unexpectedly.")


def main():
    hdfs = connect_hdfs()
    consumer = connect_kafka()
    print(f"Consuming '{TOPIC}' and writing transaction files to HDFS.")

    try:
        for message in consumer:
            transaction = message.value
            if not isinstance(transaction, dict):
                raise ValueError("Kafka message must contain a JSON object.")
            product = str(transaction.get("product_name", "")).strip()
            try:
                units = int(transaction["units_sold"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    f"Invalid transaction at Kafka offset {message.offset}."
                ) from error
            if not product or units < 0 or "\t" in product or "\n" in product:
                raise ValueError(
                    f"Invalid transaction at Kafka offset {message.offset}."
                )

            filename = (
                f"{quote(message.topic, safe='')}-"
                f"{message.partition}-{message.offset}.json"
            )
            path = f"{HDFS_INPUT_DIR.rstrip('/')}/{filename}"
            with hdfs.write(path, encoding="utf-8", overwrite=True) as output:
                transaction["product_name"] = product
                transaction["units_sold"] = units
                json.dump(transaction, output, ensure_ascii=False)
                output.write("\n")

            consumer.commit()
            print(f"Saved {filename} to HDFS.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
