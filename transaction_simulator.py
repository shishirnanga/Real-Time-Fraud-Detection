import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "amount": round(random.uniform(10, 10000), 2),
        "merchant": fake.company(),
        "location": fake.city(),
        "timestamp": datetime.now().isoformat(),
        "device": fake.user_agent(),
        "is_international": random.choice([True, False])
    }

if __name__ == "__main__":
    while True:
        transaction = generate_transaction()
        producer.send('transactions', transaction)
        print(f"Sent: {transaction}")
        time.sleep(random.uniform(0.5, 2))