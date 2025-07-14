import requests
import json
import time
import logging
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    try:
        logger.info("Fetching data from CoinLore API")
        response = requests.get('https://api.coinlore.net/api/tickers/', timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        data = response.json().get('data', [])
        logger.info(f"Received {len(data)} cryptocurrency records")
        
        for coin in data:
            try:
                # Handle missing or invalid price values
                price_str = coin.get('price_usd', '0').replace(',', '')
                price = float(price_str) if price_str and price_str != 'None' else 0.0
                
                # Ensure all required fields are present
                if not coin.get('id') or not coin.get('name'):
                    continue
                    
                # Send to Kafka
                producer.send('crypto-data', value={
                    'id': str(coin['id']),
                    'name': coin['name'],
                    'price_usd': price,
                    'ts': int(time.time() * 1000)  # Current time in milliseconds
                })
                
                # Log sample data for verification
                if coin['id'] == '90':  # Bitcoin
                    logger.info(f"Sent Bitcoin: {price}")
                    
            except Exception as e:
                logger.error(f"Error processing coin {coin.get('id')}: {str(e)}")
                
        time.sleep(60)
    except Exception as e:
        logger.error(f"API request failed: {str(e)}")
        time.sleep(30)