import requests
import json
import time
import logging
from kafka import KafkaProducer

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
                price_str = coin.get('price_usd', '0').replace(',', '')
                price = float(price_str) if price_str and price_str != 'None' else 0.0
                
                if not coin.get('id') or not coin.get('name'):
                    continue
                    
                producer.send('crypto-data', value={
                    'id': str(coin['id']),
                    'name': coin['name'],
                    'price_usd': price,
                    'ts': int(time.time() * 1000) 
                })
                
                if coin['id'] == '90':  
                    logger.info(f"Sent Bitcoin: {price}")
                    
            except Exception as e:
                logger.error(f"Error processing coin {coin.get('id')}: {str(e)}")
                
        time.sleep(60)
    except Exception as e:
        logger.error(f"API request failed: {str(e)}")
        time.sleep(30)