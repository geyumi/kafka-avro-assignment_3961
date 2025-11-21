# consumer/consumer.py
# this is whether we consume messages from kafka topic "orders" and process them
# maintain running average of order prices
import time, io, json
from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import schemaless_reader, parse_schema
from pathlib import Path
from collections import defaultdict

KAFKA_BOOTSTRAP = "localhost:9092"
GROUP_ID = "order-consumer-group"
TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# running average store in the memory. For production use external store like Redis or databases
stats = {
    "count": 0,
    "sum": 0.0
}

# load schema for decoding Avro messages from Kafka
schema_path = Path(__file__).resolve().parent.parent / "order.avsc"
schema = json.load(open(schema_path))
parsed_schema = parse_schema(schema)

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}
consumer = Consumer(consumer_conf)
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def process_order(order):
    """
    Simulate processing. Raise exception for specific cases to demonstrate retry and DLQ.
    Replace with real business logic.
    """
    # treat price < 0 as permanent failure
    if order["price"] < 0:
        raise ValueError("Permanent failure: negative price")
    # Simulate transient failure for price == 13.13 
    if 0 <= order["price"] < 10:
        raise RuntimeError("Temporary failure: price too low")
    # If OK, update running average 
    stats["count"] += 1
    stats["sum"] += order["price"]
    avg = stats["sum"] / stats["count"]
    print(f"Processed order {order['orderId']} price={order['price']:.2f} running_avg={avg:.2f}")

def send_to_dlq(raw_value, headers=None):
    producer.produce(DLQ_TOPIC, value=raw_value, headers=headers)
    producer.flush()

def handle_with_retries(raw_value):
    # to decode Avro bytes from Kafka 
    buf = io.BytesIO(raw_value)
    try:
        order = schemaless_reader(buf, schema)
    except Exception as e:
        # If decode fails, send to DLQ immediately
        print("Avro decode failed, sending to DLQ:", e)
        send_to_dlq(raw_value)
        return

    # Attempt processing with retries 
    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            process_order(order)
            return
        except RuntimeError as e:
            # temporary failure -> retry
            attempt += 1
            if attempt > MAX_RETRIES:
                print("Exceeded retries, sending to DLQ (temp failure)")
                send_to_dlq(raw_value)
                return
            backoff = RETRY_BACKOFF_SECONDS * attempt
            print(f"Temporary failure: {e}, retrying in {backoff}s (attempt {attempt})")
            time.sleep(backoff)
        except Exception as e:
            # permanent failure -> send to DLQ
            print("Permanent failure:", e)
            send_to_dlq(raw_value)
            return

def main():
    consumer.subscribe([TOPIC])
    print("Consumer started, subscribed to:", TOPIC)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: ", msg.error())
                continue
            raw = msg.value()
            # use headers to track retry attempts and metadata
            handle_with_retries(raw)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
