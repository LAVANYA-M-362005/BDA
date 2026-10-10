import csv
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales-transactions")
INPUT_FILE = Path(os.getenv("SALES_CSV", "/app/data/sales.csv"))


def main():
    if not INPUT_FILE.is_file():
        raise FileNotFoundError(f"Sales transaction CSV not found: {INPUT_FILE}")

    producer = connect_kafka()

    sent = 0
    with INPUT_FILE.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        fields = {field.strip() for field in (reader.fieldnames or [])}
        if not {"product_name", "units_sold"}.issubset(fields):
            raise ValueError(
                "Sales CSV must contain product_name and units_sold columns."
            )

        for line_number, row in enumerate(reader, start=2):
            transaction = {
                key.strip(): value for key, value in row.items() if key is not None
            }
            product_name = (transaction.get("product_name") or "").strip()
            try:
                units_sold = int(transaction.get("units_sold", ""))
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"Invalid units_sold value on CSV line {line_number}."
                ) from error
            if (
                not product_name
                or units_sold < 0
                or "\t" in product_name
                or "\n" in product_name
            ):
                raise ValueError(
                    f"CSV line {line_number} must have a single-line product "
                    "name and non-negative units_sold."
                )

            transaction["product_name"] = product_name
            transaction["units_sold"] = units_sold
            producer.send(TOPIC, transaction).get(timeout=30)
            sent += 1

    producer.flush()
    producer.close()
    if sent == 0:
        raise ValueError(f"No sales transactions found in {INPUT_FILE}.")
    print(f"Published {sent} transactions to Kafka topic '{TOPIC}'.")


def connect_kafka():
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=10000,
            )
        except KafkaError:
            if attempt == 29:
                raise
            time.sleep(2)
    raise RuntimeError("Kafka producer connection attempts ended unexpectedly.")


if __name__ == "__main__":
    main()
