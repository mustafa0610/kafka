from kafka import KafkaConsumer
import ast  # for safely evaluating string representation of dictionary

# Kafka configuration
KAFKA_TOPIC = "finance"
KAFKA_SERVER = "localhost:9092"

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8')  # Decode to a regular string
)

# Initialize a counter for serial numbers
message_counter = 0

# Continuously listen for new messages
print("Listening for messages on the finance topic...")
for message in consumer:
    try:
        # Safely evaluate the string into a dictionary
        data = ast.literal_eval(message.value)
        message_counter += 1
        print(f"Message {message_counter}: {data}")
    except (SyntaxError, ValueError) as e:
        print(f"Failed to parse message: {message.value}. Error: {e}")
