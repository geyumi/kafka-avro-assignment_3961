Kafka Avro Assignment

This project implements a complete event-driven pipeline using Apache Kafka, Avro serialization, Python producers & consumers, retry logic, dead letter queue (DLQ) and a running average computation.

🧰 Technologies Used

| Technology                             |      Purpose                              |
| ---------------------------------------|------------------------------------------ |
| **Docker & Docker Compose**            | Running Kafka, Zookeeper, Schema Registry  |
| **Apache Kafka**                       | Streaming platform                         |
| **Schema Registry**                    | Manage Avro schemas                        |
| **Python (confluent-kafka + fastavro)**| Producer & consumer logic                  |
| **Avro Serialization**                 | Compact message format & schema validation |
| **Retry Logic**                        | Handle transient errors                    |
| **Dead Letter Queue (DLQ)**            | Store permanently failed messages          |

✔️ Features Implemented

1. Avro Serialization

Messages follow the schema defined in order.avsc and are serialized using fastavro.schemaless_writer before being sent to Kafka.

2. Kafka Producer

Converts order JSON → Avro bytes
Publishes to topic orders

3. Kafka Consumer

Reads Avro bytes
Deserializes using fastavro.schemaless_reader
Processes each message
Implements retry logic for temporary failures
Sends permanently failed messages to DLQ topic: orders-dlq
Maintains a running average of prices

4. Dead Letter Queue (DLQ)

Messages that fail after retries OR fail permanently are sent to a separate topic orders-dlq.

🖥️ Windows Setup Instructions 

Step 1 — Install Required Tools

Install Docker Desktop for Windows
Install Python 3.10+
Install VS Code
Install Git for Windows

Step 2 — Open the Project

Create folder: C:\kafka-avro-assignment
Open VS Code → File → Open Folder → select this folder

Step 3 — Start Kafka Using Docker

In VS Code terminal:docker compose up -d

Step 4 — Create Topics

docker compose exec kafka bash -c \
  "kafka-topics --create --topic orders --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1"

docker compose exec kafka bash -c \
  "kafka-topics --create --topic orders-dlq --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1"

Step 5 — Install Producer Requirements

Run producer:python api.py

Step 6 — Install Consumer Requirements

Run producer:python consumer.py


🚀 Testing the Pipeline

1. Send an Order Message

Run:

curl -X POST http://127.0.0.1:5000/produce \
  -H "Content-Type: application/json" \
  -d "{\"product\":\"Item1\",\"price\":12.5}"


Producer should print: Message sent to Kafka


Consumer should print: Processed order ... running_avg=12.50

2. Test Retry Logic (Temporary Failure)

Send:

curl -X POST http://127.0.0.1:5000/produce \
  -H "Content-Type: application/json" \
  -d "{\"product\":\"Retry\",\"price\":13.13}"


Consumer prints:
Temporary failure ... retrying in 2s
Temporary failure ... retrying in 4s
...
Exceeded retries, sending to DLQ

3. Test Permanent Failure

Send:

curl -X POST http://127.0.0.1:5000/produce \
  -H "Content-Type: application/json" \
  -d "{\"product\":\"Bad\",\"price\":-1}"


Consumer output:
Permanent failure: negative price
Sending to DLQ...

4. View DLQ Messages
docker compose exec kafka bash -c \
  "kafka-console-consumer --topic orders-dlq --bootstrap-server kafka:29092 --from-beginning"


📊 Running Average

count = number of valid messages
sum = total price of valid messages
running_average = sum / count

