import time
import random
import json
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = '127.0.0.1:9092' 
TOPIC_NAME = 'telemetry-raw'
REACTORS = ['Rivne-3', 'Rivne-4', 'ZAP-5', 'ZAP-6']
NOMINAL_POWER = 980.0

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=str.encode
)

print(f"Producer запущено. Надсилаємо дані в {TOPIC_NAME}...")

while True:
    current_time = time.time()
    
    for reactor_id in REACTORS:

        power = random.uniform(NOMINAL_POWER * 0.90, NOMINAL_POWER * 0.95) 

        message = {
            "reactor_id": reactor_id,
            "power_mw": power,
            "timestamp": current_time,
        }
        
        producer.send(TOPIC_NAME, key=reactor_id, value=message)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] SENT: {reactor_id} - {power:.1f} MW")
    
    time.sleep(10)