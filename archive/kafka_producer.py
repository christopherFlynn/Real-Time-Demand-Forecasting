# kafka_producer.py
import json
import time
from confluent_kafka import Producer
from simulate_orders import generate_order

producer = Producer({'bootstrap.servers': 'localhost:9092'})
TOPIC = "orders"

def delivery_report(err, msg):
    if err is not None:
        print('❌ Delivery failed:', err)
    else:
        print(f'✅ Order delivered to {msg.topic()} [{msg.partition()}]')

if __name__ == "__main__":
    while True:
        order = generate_order()
        producer.produce(TOPIC, json.dumps(order).encode('utf-8'), callback=delivery_report)
        producer.poll(0)
        time.sleep(2)  # simulate 1 order every 2 seconds
