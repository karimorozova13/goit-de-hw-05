import uuid
import time
import random

from utils import get_kafka_producer

producer = get_kafka_producer()

my_name = "karine"
topic_name = f'{my_name}_building_sensors'


try:
    data = {
        "created_at": time.time(),
        "id": str(uuid.uuid4()),
        "temperature": random.randint(25, 45),
        "humidity": random.randint(15, 85),

    }
    producer.send(topic_name, key=str(uuid.uuid4()), value=data)
    producer.flush()
    print(f"Message sent to topic '{topic_name}' successfully.")
    time.sleep(0.5)
except Exception as e:
    print(f"An error occurred: {e}")

producer.close()

