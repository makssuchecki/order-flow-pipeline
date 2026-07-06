import asyncio, json, uuid, random, os
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "2"))

PRODUCTS = [
    ("Laptop", "Electronics", 2999),
    ("TV", "Electronics", 4999),
    ("Shoes", "Clothing", 349),
    ("Headphones", "Electronics", 449),
    ("Book", "Books", 49),
    ("Jeans", "Clothing", 199),
]
REGIONS = ["PL", "DE", "FR", "NL", "ES"]
USERS = [f"user_{i:04d}" for i in range(200)]

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

def generate_order():
    product, category, base_price = random.choice(PRODUCTS)
    amount = base_price * random.uniform(0.9, 1.1)
    if random.random() < 0.03:
        amount *= random.uniform(8, 15)
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "product": product,
        "category": category,
        "amount": round(amount, 2),
        "region": random.choice(REGIONS),
        "status": "placed",
        "timestamp": datetime.utcnow().isoformat(),
    }

async def run():
    delay = 1.0 / EVENTS_PER_SECOND
    print(f"Generator started — {EVENTS_PER_SECOND} events/s → {KAFKA_BROKER}")
    while True:
        order = generate_order()
        producer.produce("orders", json.dumps(order).encode())
        producer.poll(0)
        print(f"→ [{order['region']}] {order['product']} ${order['amount']:.2f}")
        await asyncio.sleep(delay)

asyncio.run(run())