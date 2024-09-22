# Kafka ETL Pipeline

This project shows how to set up a simple Kafka-based ETL pipeline using Docker Compose for Kafka infrastructure and Python scripts for producing and consuming messages. It simulates a finance topic where the producer sends financial transaction data, and the consumer listens to and processes it.

## What's included:

1. **docker-compose.yml**: Sets up Kafka and Zookeeper with Docker Compose.
2. **finance_producer.py**: A Python script that sends (produces) financial data to a Kafka topic.
3. **finance_consumer.py**: A Python script that listens (consumes) data from the Kafka topic.

## How to run it:

### 1. Start Kafka with Docker Compose:

First, make sure you’ve got Docker installed. Then, navigate to the folder with your `docker-compose.yml` file and spin up Kafka and Zookeeper:

```bash
docker-compose up -d
```

### 2. Create a Kafka topic:

With Kafka running, you need to create a topic (like `finance_transactions`). Run this:

```bash
docker exec -it <kafka-container-id> kafka-topics --create --topic finance_transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Replace `<kafka-container-id>` with your actual Kafka container ID, which you can find by running:

```bash
docker ps
```

### 3. Run the Producer:

Now let’s produce some messages (financial data) into the `finance_transactions` topic. Just run:

```bash
python finance_producer.py
```

### 4. Run the Consumer:

To consume those messages from the topic, run the consumer script:

```bash
python finance_consumer.py
```

### 5. Check Kafka logs:

If you want to check the logs or debug anything, you can view the Kafka logs with:

```bash
docker-compose logs kafka
```


