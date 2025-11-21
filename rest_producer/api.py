# rest_producer/api.py
import json, uuid
from flask import Flask, request, jsonify
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from fastavro import parse_schema
from pathlib import Path

app = Flask(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"

# Load Avro schema
schema_path = Path(__file__).resolve().parent.parent / "order.avsc"
schema = json.load(open(schema_path))
parsed_schema = parse_schema(schema)

schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
sr_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(schema, sr_client, to_dict=lambda obj, ctx: obj)

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP
}
producer = Producer(producer_conf)

@app.route("/produce", methods=["POST"])
def produce():
    payload = request.json
    # Ensure fields: orderId, product, price
    if "orderId" not in payload:
        payload["orderId"] = str(uuid.uuid4())
    # Convert price to float
    payload["price"] = float(payload["price"])
    # Serialize using fastavro before sending or use AvroSerializer to produce bytes
    # We'll register the schema in the registry and produce raw Avro bytes with a simple approach:
    from fastavro import writer, schemaless_writer
    import io
    buf = io.BytesIO()
    schemaless_writer(buf, schema, payload)
    avro_bytes = buf.getvalue()

    # send as bytes to Kafka
    producer.produce(TOPIC, value=avro_bytes)
    producer.flush()
    return jsonify({"status":"sent", "order": payload})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
