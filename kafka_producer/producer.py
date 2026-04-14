"""
Kafka Producer for Retail Transaction Events.
Generates realistic synthetic transactions using Faker, publishes to Kafka.
Includes edge cases: nulls, duplicates, late arrivals.
"""
import json, logging, random, time, uuid
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import KAFKA_CONFIG, PRODUCER_CONFIG, PRODUCT_CATEGORIES, STORE_REGIONS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("RetailProducer")
fake = Faker()
Faker.seed(42)


class RetailTransactionProducer:
    def __init__(self):
        self.producer = self._create_producer()
        self.products = self._generate_product_catalog()
        self.stores = self._generate_store_list()
        self.sent_count = 0
        self.error_count = 0

    def _create_producer(self) -> KafkaProducer:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
                client_id=KAFKA_CONFIG["client_id"],
                acks=KAFKA_CONFIG["acks"],
                retries=KAFKA_CONFIG["retries"],
                retry_backoff_ms=KAFKA_CONFIG["retry_backoff_ms"],
                batch_size=KAFKA_CONFIG["batch_size"],
                linger_ms=KAFKA_CONFIG["linger_ms"],
                compression_type=KAFKA_CONFIG["compression_type"],
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        except KafkaError as e:
            logger.error("Failed to create producer: %s", e)
            raise

    def _generate_product_catalog(self):
        return [{"product_id": f"PROD-{i+1:04d}", "product_name": fake.catch_phrase(),
                 "category": random.choice(PRODUCT_CATEGORIES),
                 "unit_price": round(random.uniform(1.99, 499.99), 2)}
                for i in range(PRODUCER_CONFIG["num_products"])]

    def _generate_store_list(self):
        return [{"store_id": f"STORE-{i+1:03d}", "store_name": f"RetailMart #{i+1}",
                 "region": random.choice(STORE_REGIONS), "state": fake.state_abbr(),
                 "city": fake.city()}
                for i in range(PRODUCER_CONFIG["num_stores"])]

    def _generate_transaction(self):
        product = random.choice(self.products)
        store = random.choice(self.stores)
        qty = random.randint(1, 20)
        ts = datetime.utcnow()
        if random.random() < PRODUCER_CONFIG["late_arrival_probability"]:
            ts -= timedelta(minutes=random.randint(1, PRODUCER_CONFIG["late_arrival_max_minutes"]))
        txn = {
            "order_id": str(uuid.uuid4()), "product_id": product["product_id"],
            "product_name": product["product_name"], "category": product["category"],
            "quantity": qty, "unit_price": product["unit_price"],
            "total_amount": round(qty * product["unit_price"], 2),
            "store_id": store["store_id"], "store_name": store["store_name"],
            "region": store["region"], "state": store["state"], "city": store["city"],
            "payment_method": random.choice(["credit_card", "debit_card", "cash", "mobile_pay"]),
            "customer_segment": random.choice(["regular", "premium", "new", "loyalty"]),
            "transaction_timestamp": ts.isoformat(), "event_type": "purchase",
        }
        if random.random() < PRODUCER_CONFIG["null_probability"]:
            txn[random.choice(["product_name", "category", "payment_method"])] = None
        return txn

    def _generate_duplicate(self, txn):
        dup = txn.copy()
        dup["_is_duplicate"] = True
        return dup

    def produce_events(self, max_events=None):
        topic = KAFKA_CONFIG["topic"]
        logger.info("Starting production to topic: %s", topic)
        count = 0
        try:
            while max_events is None or count < max_events:
                txn = self._generate_transaction()
                self.producer.send(topic, key=txn["store_id"], value=txn)
                self.sent_count += 1
                count += 1
                if random.random() < PRODUCER_CONFIG["duplicate_probability"]:
                    self.producer.send(topic, key=txn["store_id"], value=self._generate_duplicate(txn))
                    count += 1
                time.sleep(random.uniform(PRODUCER_CONFIG["min_delay_seconds"], PRODUCER_CONFIG["max_delay_seconds"]))
        except KeyboardInterrupt:
            pass
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Done. Sent: %d | Errors: %d", self.sent_count, self.error_count)


if __name__ == "__main__":
    RetailTransactionProducer().produce_events()
