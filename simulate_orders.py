# simulate_orders.py
import random
import uuid
from datetime import datetime
from faker import Faker

fake = Faker()

REGIONS = ['Northeast', 'Midwest', 'South', 'West']
PRODUCT_IDS = ['SKU-001', 'SKU-002', 'SKU-003', 'SKU-004']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']

def generate_order():
    order = {
        "timestamp": datetime.utcnow().isoformat(),
        "order_id": f"ORD-{uuid.uuid4().hex[:10]}",
        "product_id": random.choice(PRODUCT_IDS),
        "user_id": f"USER-{uuid.uuid4().hex[:8]}",
        "region": random.choice(REGIONS),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10, 100), 2),
        "device_type": random.choice(DEVICE_TYPES),
        "promo_applied": random.choice([True, False])
    }
    order["total_price"] = round(order["quantity"] * order["unit_price"], 2)
    return order

if __name__ == "__main__":
    print(generate_order())  # Just for testing
