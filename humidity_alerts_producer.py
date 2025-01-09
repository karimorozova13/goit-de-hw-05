import uuid
import time

from utils import get_kafka_producer


def produce_humidity_alerts(data) -> None:
    producer = get_kafka_producer()

    my_name = "kari"
    topic_name = f'{my_name}_humidity_alerts'

    try:
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()
        print(f"Message sent to topic '{topic_name}' successfully.")
        time.sleep(0.5)
    except Exception as e:
        print(f"An error occurred: {e}")

    producer.close()

if __name__ == "__main__":
    produce_humidity_alerts()

