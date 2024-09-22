import requests
import json
from kafka import KafkaProducer
import time

# api
API_KEY = "####"  # Your actual API key
BASE_URL = "https://www.alphavantage.co/query"

# kafka configuration
KAFKA_TOPIC = "finance"
KAFKA_SERVER = "localhost:9092"

# initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_stock_data(symbol, retries=3):
    """Fetch stock price data from Alpha Vantage with retry."""
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",
        "apikey": API_KEY
    }
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()  # check for HTTP errors
            data = response.json()

            if "Time Series (1min)" in data:
                time_series = data["Time Series (1min)"]
                latest_time = next(iter(time_series))
                return {latest_time: time_series[latest_time]}
            else:
                return {"error": "No data available"}
        except (requests.ConnectionError, requests.Timeout) as e:
            print(f"Connection error: {e}. Retrying {retries - attempt - 1} more times...")
            attempt += 1
            time.sleep(5)  # wait before retrying
        except requests.RequestException as e:
            print(f"Failed to retrieve data: {e}")
            break

    return {"error": "Failed after retries"}

def produce_financial_data(symbol):
    """Fetch data from API and produce it to Kafka."""
    stock_data = get_stock_data(symbol)

    if "error" not in stock_data:
        # Send data to Kafka
        producer.send(KAFKA_TOPIC, value=stock_data)
        print(f"Produced data: {stock_data}")
    else:
        print(stock_data["error"])

if __name__ == "__main__":
    stock_symbol = "AAPL"  
    while True:
        produce_financial_data(stock_symbol)
        time.sleep(60)  # every minute
